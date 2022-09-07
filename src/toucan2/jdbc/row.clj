(ns toucan2.jdbc.row
  "Custom [[TransientRow]] type. This is mostly in a separate namespace so I don't have to look at it when working on
  unrelated [[toucan2.jdbc.result-set]] stuff.

  This is roughly adapted from [[next.jdbc.result-set/mapify-result-set]] in a somewhat-successful attempt to make
  Toucan 2 be [[next.jdbc]]-compatible."
  (:require
   [better-cond.core :as b]
   [clojure.core.protocols :as core-p]
   [clojure.datafy :as d]
   [clojure.pprint :as pprint]
   [clojure.string :as str]
   [next.jdbc.result-set :as next.jdbc.rs]
   [puget.printer :as puget]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize])
  (:import
   (java.sql ResultSet ResultSetMetaData)))

(set! *warn-on-reflection* true)

(declare print-representation-parts)

(defn- fetch-column-with-name
  "Fetch the column with `column-name`. Returns `not-found` if no such column exists."
  [builder i->thunk column-name not-found]
  ;; this might get called with some other non-string or non-keyword key, in that case just return `not-found`
  ;; immediately since we're not going to find it by hitting the database.
  (if (and (not (string? column-name))
           (not (instance? clojure.lang.Named column-name)))
    (do
      (log/tracef :results "Can't fetch column with name %s. Returning not found value %s" column-name not-found)
      not-found)
    (let [label-fn     (get-in builder [:opts :label-fn])
          _            (assert (fn? label-fn) "Builder should have :opts with :label-fn")
          column-name' (keyword
                        (when (instance? clojure.lang.Named column-name)
                          (when-let [col-ns (namespace column-name)]
                            (label-fn (name col-ns))))
                        (label-fn (name column-name)))]
      (log/tracef :results "Fetch column with name %s (originally %s)" column-name' column-name)
      (let [i      (when column-name'
                     (first (keep-indexed
                             (fn [i col]
                               (when (= col column-name')
                                 (inc i)))
                             (:cols builder))))
            result (b/cond
                     (not i)     not-found
                     :let        [thunk (i->thunk i)]
                     (not thunk) not-found
                     :else       (thunk))]
        (log/tracef :results "=> %s" result)
        result))))

(def ^:private ^:dynamic *fetch-all-columns* true)

;;; One of these is built for every row in the results.
(deftype ^:no-doc TransientRow [model
                                ^ResultSet rset
                                ^ResultSetMetaData rsmeta
                                current-row-num
                                ;; [[next.jdbc]] result set builder, usually an instance
                                ;; of [[toucan2.jdbc.result_set.InstanceBuilder]] or
                                ;; whatever [[toucan2.jdbc.result-set/builder-fn]] returns. Should have the key `:cols`
                                builder
                                ;; an atom with a set of realized column names.
                                realized-transient-keys
                                ;; given a JDBC column index (starting at 1) return a thunk that can be used to fetch
                                ;; the column. This usually comes from [[toucan2.jdbc.read/make-cached-i->thunk]].
                                i->thunk
                                ;; underlying transient map representing this row.
                                ^clojure.lang.ITransientMap transient-row
                                ;; an atom recording whether we've already been realized. If we have, this contains a
                                ;; stacktrace to the place that we got realized.
                                already-realized?
                                ;; a delay that should return a persistent map for the current row. Once this is called
                                ;; we should return the realized row directly and work with that going forward.
                                realized-row]
  next.jdbc.result_set.InspectableMapifiedResultSet
  (row-number   [_this] current-row-num)
  (column-names [_this] (:cols builder))
  (metadata     [_this] (d/datafy rsmeta))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    (log/tracef :results ".assoc %s %s" k v)
    (if @already-realized?
      (assoc @realized-row k v)
      (do
        (swap! realized-transient-keys conj k)
        (assoc! transient-row k v)
        this)))
;;; TODO -- can we `assocEx` the transient row?
  (assocEx [_this k v]
    (log/tracef :results ".assocEx %s %s" k v)
    (.assocEx ^clojure.lang.IPersistentMap @realized-row k v))
  (without [this k]
    (log/tracef :results ".without %s" k)
    (if @already-realized?
      (dissoc @realized-row k)
      (do
        (dissoc! transient-row k)
        (swap! realized-transient-keys disj k)
        this)))

  ;; Java 7 compatible: no forEach / spliterator
  ;;
  ;; TODO -- not sure if we need/want this
  java.lang.Iterable
  (iterator [_this]
    (log/tracef :results ".iterator")
    (.iterator ^java.lang.Iterable @realized-row))

  clojure.lang.Associative
  (containsKey [this k]
    (log/tracef :results ".containsKey %s" k)
    (not= (.valAt this k ::not-found) ::not-found))
  (entryAt [this k]
    (log/tracef :results ".entryAt %s" k)
    (let [v (.valAt this k ::not-found)]
      (when-not (= v ::not-found)
        (clojure.lang.MapEntry. k v))))

  clojure.lang.Counted
  (count [_this]
    (log/tracef :results ".count")
    (let [cols (:cols builder)]
      (assert (seq cols))
      (count cols)))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (log/tracef :results ".cons %s" o)
    (cond
      (map? o)
      (reduce #(apply assoc %1 %2) this o)

      (instance? java.util.Map o)
      (reduce #(apply assoc %1 %2) this (into {} o))

      :else
      (if-let [[k v] (seq o)]
        (assoc this k v)
        this)))
  (empty [_this]
    (log/tracef :results ".empty")
    (instance/empty-map model))
  (equiv [_this obj]
    (log/tracef :results ".equiv %s" obj)
    (.equiv ^clojure.lang.IPersistentCollection @realized-row obj))

  ;; we support get with a numeric key for array-based builders:
  clojure.lang.ILookup
  (valAt [this k]
    (log/tracef :results ".valAt %s" k)
    (.valAt this k nil))
  (valAt [this k not-found]
    (log/tracef :results ".valAt %s %s" k not-found)
    (cond
      @already-realized?
      (get @realized-row k not-found)

      (number? k)
      (let [i (inc k)]
        (if-let [thunk (i->thunk i)]
          (thunk)
          not-found))

      ;; non-number column name
      :else
      (let [existing-value (.valAt transient-row k ::not-found)]
        (if-not (= existing-value ::not-found)
          existing-value
          (let [fetched-value (fetch-column-with-name builder i->thunk k ::not-found)]
            (if (= fetched-value ::not-found)
              not-found
              (do
                (.assoc this k fetched-value)
                fetched-value)))))))

  ;; we support nth for array-based builderset (i is primitive int here!):
  ;; clojure.lang.Indexed
  ;; (nth [_this i]
  ;;   (log/tracef :results ".nth %s" i)
  ;;   (try
  ;;     (i->thunk (inc i))
  ;;     (catch java.sql.SQLException _)))
  ;; (nth [_this i not-found]
  ;;   (log/tracef :results ".nth %s %s" i not-found)
  ;;   (try
  ;;     (i->thunk (inc i))
  ;;     (catch java.sql.SQLException _
  ;;       not-found)))

  clojure.lang.Seqable
  (seq [_this]
    (log/tracef :results ".seq")
    (seq @realized-row))

  ;; calling `persistent!` on a transient row will convert it to a persistent object WITHOUT realizing all the columns.
  clojure.lang.ITransientCollection
  (persistent [_this]
    (log/tracef :results ".persistent")
    (binding [*fetch-all-columns* false]
      @realized-row))

  next.jdbc.rs/DatafiableRow
  (datafiable-row [_this connectable opts]
    ;; since we have to call these eagerly, we trap any exceptions so
    ;; that they can be thrown when the actual functions are called
    (let [row   (try (.getRow rset)  (catch Throwable t t))
          cols  (try (:cols builder) (catch Throwable t t))
          metta (try (d/datafy rsmeta) (catch Throwable t t))]
      (vary-meta
       @realized-row
       assoc
       `core-p/datafy (#'next.jdbc.rs/navize-row connectable opts)
       `core-p/nav    (#'next.jdbc.rs/navable-row connectable opts)
       `row-number    (fn [_this] (if (instance? Throwable row) (throw row) row))
       `column-names  (fn [_this] (if (instance? Throwable cols) (throw cols) cols))
       `metadata      (fn [_this] (if (instance? Throwable metta) (throw metta) metta)))))

  protocols/IModel
  (model [_this]
    model)

  protocols/IRealizedKeys
  (realized-keys [_this]
    @realized-transient-keys)

  realize/Realize
  (realize [_this]
    @realized-row)

  (toString [this]
    (str/join \space (map str (print-representation-parts this)))))

;;; We don't use [[pretty.core/PrettyPrintable]] for this like we do for everything else because we want to print TWO
;;; things, the [[print-symbol]] and a map.

(defn- print-representation-parts
  "Returns a sequence of things to print to represent a [[TransientRow]]. Avoids realizing the entire row if we're still in
  'transient' mode."
  [^toucan2.jdbc.row.TransientRow row]
  (try
    (let [transient-row           (.transient_row row)
          realized-transient-keys (.realized_transient_keys row)]
      [(symbol (format "^%s " `TransientRow))
       ;; (instance? pretty.core.PrettyPrintable transient-row) (pretty/pretty transient-row)
       (zipmap @realized-transient-keys
               (map #(get transient-row %) @realized-transient-keys))])
    (catch Exception _
      ["unrealized result set {row} -- do you need to call toucan2.realize/realize ?"])))

(defmethod print-method toucan2.jdbc.row.TransientRow
  [row writer]
  (doseq [part (print-representation-parts row)]
    (print-method part writer)))

(defmethod pprint/simple-dispatch toucan2.jdbc.row.TransientRow
  [row]
  (doseq [part (print-representation-parts row)]
    (pprint/simple-dispatch part)))

(defmethod log/print-handler toucan2.jdbc.row.TransientRow
  [_klass]
  (fn [printer row]
    (for [part (print-representation-parts row)]
      (puget/format-doc printer part))))

(doseq [methodd [print-method
                 pprint/simple-dispatch]]
  (prefer-method methodd toucan2.jdbc.row.TransientRow clojure.lang.IPersistentMap))

;;; A lot of the stuff below is an adapted/custom version of the code in [[next.jdbc.result-set]] -- I would have
;;; preferred to not have to do this but a lot of it was necessary to make things work in the Toucan 2 work. See this
;;; Slack thread for more information: https://clojurians.slack.com/archives/C1Q164V29/p1662494291800529

(defn- fetch-all-columns! [builder transient-row]
  (log/tracef :results "Fetching all columns")
  (reduce (fn [^clojure.lang.ITransientMap transient-row i]
            ;; make sure the key is not already present. If it is we don't want to stomp over existing values.
            (let [col-name (nth (:cols builder) (dec i))]
              (if (= (.valAt transient-row col-name ::not-found) ::not-found)
                (next.jdbc.rs/with-column builder transient-row i)
                transient-row)))
          transient-row
          (range 1 (inc (next.jdbc.rs/column-count builder)))))

(defn- make-realized-row-delay [builder transient-row]
  (delay
    (log/tracef :results "Fully realizing row")
    (when *fetch-all-columns*
      (fetch-all-columns! builder transient-row))
    (next.jdbc.rs/row! builder transient-row)))

(defn row
  [model ^ResultSet rset i->thunk row-num opts]
  (assert (not (.isClosed rset)) "ResultSet is already closed")
  (let [rsmeta             (.getMetaData rset)
        builder            ((get opts :builder-fn next.jdbc.rs/as-maps) rset opts)
        transient-row      (next.jdbc.rs/->row builder)
        realized-row-delay (make-realized-row-delay builder transient-row)
        already-realized?  (atom false)
        realized-row-delay (delay
                             (reset! already-realized? true)
                             @realized-row-delay)]
    (->TransientRow model rset rsmeta row-num builder (atom #{}) i->thunk transient-row already-realized? realized-row-delay)))
