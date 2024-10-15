(ns ^:no-doc toucan2.jdbc.row
  "Custom [[TransientRow]] type. This is mostly in a separate namespace so I don't have to look at it when working on
  unrelated [[toucan2.jdbc.result-set]] stuff.

  This is roughly adapted from [[next.jdbc.result-set/mapify-result-set]] in a somewhat-successful attempt to make
  Toucan 2 be `next.jdbc`-compatible."
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
   [toucan2.realize :as realize]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet)))

(set! *warn-on-reflection* true)

(declare print-representation-parts)

(defn- fetch-column-with-name
  "Fetch the column with `column-name`. Returns `not-found` if no such column exists."
  [column-name->index i->thunk column-name not-found]
  ;; this might get called with some other non-string or non-keyword key, in that case just return `not-found`
  ;; immediately since we're not going to find it by hitting the database.
  (let [i      (column-name->index column-name)
        result (b/cond
                 (not i)     not-found
                 :let        [thunk (i->thunk i)]
                 (not thunk) not-found
                 :else       (thunk))]
    (log/tracef "=> %s" result)
    result))

(def ^:private ^:dynamic *fetch-all-columns* true)

;;; One of these is built for every row in the results.
;;;
;;; TODO -- maybe we can combine the
(deftype ^:no-doc TransientRow [model
                                ^ResultSet rset
                                ;; `next.jdbc` result set builder, usually an instance
                                ;; of [[toucan2.jdbc.result_set.InstanceBuilder]] or
                                ;; whatever [[toucan2.jdbc.result-set/builder-fn]] returns. Should have the key `:cols`
                                builder
                                ;; a function that given a column name key will normalize it and return the
                                ;; corresponding JDBC index. This should probably be memoized for the whole result set.
                                column-name->index
                                ;; a list of all column names
                                column-names
                                ;; ATOM with map. Given a JDBC column index (starting at 1) return a thunk that can be
                                ;; used to fetch the column. This usually comes
                                ;; from [[toucan2.jdbc.read/make-cached-i->thunk]].
                                i->thunk
                                ;; a Volatile that contains the underlying transient map representing this row.
                                ^clojure.lang.Volatile volatile-transient-row
                                ;; a delay that should return a persistent map for the current row. Once this is called
                                ;; we should return the realized row directly and work with that going forward.
                                realized-row]
  next.jdbc.result_set.InspectableMapifiedResultSet
  (row-number   [_this] (.getRow rset))
  (column-names [_this] (:cols builder))
  (metadata     [_this] (d/datafy (.getMetaData rset)))

  clojure.lang.IPersistentMap
  (assoc [this k v]
    (log/tracef ".assoc %s %s" k v)
    (if (realized? realized-row)
      (assoc @realized-row k v)
      (do
        (vswap! volatile-transient-row (fn [^clojure.lang.ITransientMap transient-row]
                                         (let [^clojure.lang.ITransientMap transient-row' (assoc! transient-row k v)]
                                           (assert (= (.valAt transient-row' k) v)
                                                   (format "assoc! did not do what we expected. k = %s v = %s row = %s .valAt = %s"
                                                           (pr-str k)
                                                           (pr-str v)
                                                           (pr-str transient-row')
                                                           (pr-str (.valAt transient-row' k))))
                                           transient-row')))
        this)))

  ;; TODO -- can we `assocEx` the transient row?
  (assocEx [_this k v]
    (log/tracef ".assocEx %s %s" k v)
    (.assocEx ^clojure.lang.IPersistentMap @realized-row k v))

  (without [this k]
    (log/tracef ".without %s" k)
    (if (realized? realized-row)
      (dissoc @realized-row k)
      (do
        (vswap! volatile-transient-row dissoc! k)
        (let [k-index             (column-name->index k)
              column-name->index' (if-not k-index
                                    column-name->index
                                    (fn [column-name]
                                      (let [index (column-name->index column-name)]
                                        (when-not (= index k-index)
                                          index))))]
          (when k-index
            (swap! i->thunk (fn [i->thunk]
                              (fn [index]
                                (if (= index k-index)
                                  (constantly ::not-found)
                                  (i->thunk index))))))
          (if (identical? column-name->index column-name->index')
            this
            (TransientRow. model
                           rset
                           builder
                           column-name->index'
                           column-names
                           i->thunk
                           volatile-transient-row
                           realized-row))))))

  ;; Java 7 compatible: no forEach / spliterator
  ;;
  ;; TODO -- not sure if we need/want this
  java.lang.Iterable
  (iterator [_this]
    (log/tracef ".iterator")
    (.iterator ^java.lang.Iterable @realized-row))

  clojure.lang.Associative
  (containsKey [_this k]
    (log/tracef ".containsKey %s" k)
    (if (realized? realized-row)
      (contains? @realized-row k)
      (or (contains? @volatile-transient-row k)
          (boolean (column-name->index k)))))

  (entryAt [this k]
    (log/tracef ".entryAt %s" k)
    (let [v (.valAt this k ::not-found)]
      (when-not (= v ::not-found)
        (clojure.lang.MapEntry. k v))))

  ;; TODO -- this should probably also include any extra keys added with `assoc` or whatever
  clojure.lang.Counted
  (count [_this]
    (log/tracef ".count")
    (let [cols (:cols builder)]
      (assert (seq cols))
      (count cols)))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (log/tracef ".cons %s" o)
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
    (log/tracef ".empty")
    (instance/instance model))

  (equiv [_this obj]
    (log/tracef ".equiv %s" obj)
    (.equiv ^clojure.lang.IPersistentCollection @realized-row obj))

  ;; we support get with a numeric key for array-based builders:
  clojure.lang.ILookup
  (valAt [this k]
    (log/tracef ".valAt %s" k)
    (.valAt this k nil))

  (valAt [this k not-found]
    (log/tracef ".valAt %s %s" k not-found)
    (cond
      (realized? realized-row)
      (get @realized-row k not-found)

      (number? k)
      (let [i (inc k)]
        (if-let [thunk (@i->thunk i)]
          (thunk)
          not-found))

      ;; non-number column name
      :else
      (let [existing-value (.valAt ^clojure.lang.ITransientMap @volatile-transient-row k ::not-found)]
        (if-not (= existing-value ::not-found)
          existing-value
          (let [fetched-value (fetch-column-with-name column-name->index @i->thunk k ::not-found)]
            (if (= fetched-value ::not-found)
              not-found
              (do
                (.assoc this k fetched-value)
                fetched-value)))))))

  clojure.lang.Seqable
  (seq [_this]
    (log/tracef ".seq")
    (seq @realized-row))

  ;; calling [[persistent!]] on a transient row will convert it to a persistent object WITHOUT realizing all the columns.
  clojure.lang.ITransientCollection
  (persistent [_this]
    (log/tracef ".persistent")
    (binding [*fetch-all-columns* false]
      @realized-row))

  next.jdbc.rs/DatafiableRow
  (datafiable-row [_this connectable opts]
    ;; since we have to call these eagerly, we trap any exceptions so
    ;; that they can be thrown when the actual functions are called
    (let [row   (try (.getRow rset)  (catch Throwable t t))
          cols  (try (:cols builder) (catch Throwable t t))
          metta (try (d/datafy (.getMetaData rset)) (catch Throwable t t))]
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

  protocols/IDispatchValue
  (dispatch-value [_this]
    (protocols/dispatch-value model))

  protocols/IDeferrableUpdate
  (deferrable-update [this k f]
    (log/tracef "Doing deferrable update of %s with %s" k f)
    (b/cond
      (realized? realized-row)
      (update @realized-row k f)

      :let [existing-value (.valAt ^clojure.lang.ITransientMap @volatile-transient-row k ::not-found)]

      ;; value already exists: update the value in the transient row and call it a day
      (not= existing-value ::not-found)
      (assoc this k (f existing-value))

      ;; otherwise compose the column thunk with `f`
      :else
      (let [col-index (column-name->index k)]
        (assert col-index (format "No column named %s in results. Got: %s" (pr-str k) (pr-str (:cols builder))))
        (swap! i->thunk (fn [i->thunk]
                          (fn [i]
                            (let [thunk (i->thunk i)]
                              (if (= i col-index)
                                (comp f thunk)
                                thunk)))))
        this)))

  realize/Realize
  (realize [_this]
    @realized-row)

  u/IsCustomMap
  (custom-map? [_] true)

  (toString [this]
    (str/join \space (map str (print-representation-parts this)))))

;;; We don't use [[pretty.core/PrettyPrintable]] for this like we do for everything else because we want to print TWO
;;; things, the [[print-symbol]] and a map.

(defn- print-representation-parts
  "Returns a sequence of things to print to represent a [[TransientRow]]. Avoids realizing the entire row if we're still in
  'transient' mode."
  [^toucan2.jdbc.row.TransientRow row]
  (try
    (let [transient-row @(.volatile_transient_row row)
          column-names (.column_names row)]
      [(symbol (format "^%s " `TransientRow))
       ;; (instance? pretty.core.PrettyPrintable transient-row) (pretty/pretty transient-row)
       (into {} (keep (fn [col] (let [v (get transient-row col ::not-found)]
                                  (when-not (= v ::not-found)
                                    [col (get transient-row col)]))))
             column-names)])
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

(defn- fetch-column! [builder i->thunk ^clojure.lang.ITransientMap transient-row i]
  ;; make sure the key is not already present. If it is we don't want to stomp over existing values.
  (let [col-name (nth (:cols builder) (dec i))]
    (if (= (.valAt transient-row col-name ::not-found) ::not-found)
      (let [thunk (@i->thunk i)]
        (assert (fn? thunk))
        (let [value (thunk)]
          (if (= value ::not-found)
            transient-row
            (next.jdbc.rs/with-column-value builder transient-row col-name value))))
      transient-row)))

(defn- fetch-all-columns! [builder i->thunk transient-row]
  (log/tracef "Fetching all columns")
  (let [n (next.jdbc.rs/column-count builder)]
    (loop [i 1
           transient-row transient-row]
      (if (<= i n)
        (recur (inc i) (fetch-column! builder i->thunk transient-row i))
        transient-row))))

(defn- make-realized-row-delay [builder i->thunk ^clojure.lang.Volatile volatile-transient-row]
  (delay
    (log/tracef "Fully realizing row. *fetch-all-columns* = %s" *fetch-all-columns*)
    (let [row (cond->> @volatile-transient-row
                *fetch-all-columns* (fetch-all-columns! builder i->thunk))]
      (next.jdbc.rs/row! builder row))))

(defn ^:no-doc row
  "Create a new `TransientRow`. Part of the low-level implementation of the JDBC query execution backend. You probably
  shouldn't be using this directly!"
  [model ^ResultSet rset builder i->thunk col-name->index column-names]
  (assert (not (.isClosed rset)) "ResultSet is already closed")
  (let [volatile-transient-row (volatile! (next.jdbc.rs/->row builder))
        i->thunk               (atom i->thunk)
        realized-row-delay     (make-realized-row-delay builder i->thunk volatile-transient-row)]
    ;; this is a gross amount of positional args. But using `reify` makes debugging things too hard IMO.
    (->TransientRow model
                    rset
                    builder
                    col-name->index
                    column-names
                    i->thunk
                    volatile-transient-row
                    realized-row-delay)))
