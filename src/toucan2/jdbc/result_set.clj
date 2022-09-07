(ns toucan2.jdbc.result-set
  (:require
   [clojure.core.protocols :as core-p]
   [clojure.datafy :as d]
   [clojure.pprint :as pprint]
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [toucan2.instance :as instance]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.util :as u])
  (:import
   (java.sql Connection ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

(def type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

  ```clj
  (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\"
  ```"
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^Connection conn model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i])}
  (fn [^Connection conn model _rset ^ResultSetMetaData rsmeta ^Long i]
    (let [col-type (.getColumnType rsmeta i)]
      (log/debugf :results
                  "Column %s %s is of JDBC type %s, native type %s"
                  i
                  (.getColumnLabel rsmeta i)
                  (symbol "java.sql.Types" (type-name col-type))
                  (.getColumnTypeName rsmeta i))
      [(protocols/dispatch-value conn) (protocols/dispatch-value model) col-type])))

(m/defmethod read-column-thunk :default
  [_conn _model ^ResultSet rset _rsmeta ^Long i]
  (log/debugf :results "Fetching values in column %s with %s" i (list '.getObject 'rs i))
  (fn default-read-column-thunk []
    (log/tracef :results "col %s => %s" i (list '.getObject 'rset i))
    (.getObject rset i)))

(m/defmethod read-column-thunk :after :default
  [_conn model _rset _rsmeta thunk]
  (fn []
    (u/try-with-error-context ["read column" {:thunk thunk, :model model}]
      (thunk))))

(defn get-object-of-class-thunk [^ResultSet rset ^Long i ^Class klass]
  (log/debugf :results
              "Fetching values in column %s with %s"
              i
              (list '.getObject 'rs i klass))
  (fn get-object-of-class-thunk []
    (log/tracef :results "col %s => %s" i (list '.getObject 'rset i klass))
    (.getObject rset i klass)))

;;;; Default column read methods

(m/defmethod read-column-thunk [:default :default Types/CLOB]
  [_conn _model ^ResultSet rset _ ^Long i]
  (fn get-string-thunk []
    (.getString rset i)))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP_WITH_TIMEZONE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.OffsetDateTime))

(m/defmethod read-column-thunk [:default :default Types/DATE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDate))

(m/defmethod read-column-thunk [:default :default Types/TIME]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalTime))

(m/defmethod read-column-thunk [:default :default Types/TIME_WITH_TIMEZONE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.OffsetTime))

(defn read-column-by-index-fn [conn model]
  (let [i->thunk* (memoize (fn [^ResultSet rset ^Integer i]
                            (let [rsmeta (.getMetaData rset)]
                              (comp (fn [v]
                                      (next.jdbc.rs/read-column-by-index v rsmeta i))
                                    (read-column-thunk conn model rset rsmeta i)))))]
    (fn [_builder ^ResultSet rset ^Integer i]
      (let [thunk  (i->thunk* rset i)
            result (thunk)]
        (log/tracef :results "col %s => %s" i result)
        result))))

(m/defmulti builder-fn
  {:arglists '([conn₁ model₂])}
  u/dispatch-on-first-two-args)

(defrecord ^:no-doc InstanceBuilder [model ^ResultSet rset ^ResultSetMetaData rsmeta cols]
  next.jdbc.rs/RowBuilder
  (->row [_this]
    (log/tracef :results "Fetching row %s" (.getRow rset))
    (transient (instance/instance model)))
  (column-count [_this]
    (count cols))
  ;; this is purposefully not implemented because we should never get here; if we do it is an error and we want an
  ;; Exception thrown.
  #_(with-column [this row i]
      (println (pr-str (list 'with-column 'this 'row i)))
      (next.jdbc.rs/with-column-value this row (nth cols (dec i))
        (next.jdbc.rs/read-column-by-index (.getObject rset ^Integer i) rsmeta i)))
  (with-column-value [_this row col v]
    (assert (some? col) "Invalid col")
    (assoc! row col v))
  (row! [_this row]
    (persistent! row))

  next.jdbc.rs/ResultSetBuilder
  (->rs [_this]
    (transient []))
  (with-row [_this acc row]
    (conj! acc row))
  (rs! [_this acc]
    (persistent! acc)))

(defn instance-builder-fn [model]
  (fn [^ResultSet rset opts]
    (let [key-xform      (instance/key-transform-fn model)
          _              (log/debugf :results "Using key xform fn %s" key-xform)
          table-name->ns (model/table-name->namespace model)
          _              (log/debugf :results "Using table namespaces %s" table-name->ns)
          label-fn       (comp name key-xform)
          opts           (merge {:label-fn     label-fn
                                 :qualifier-fn (memoize
                                                (fn [table]
                                                  (let [table (name (key-xform table))]
                                                    (some-> (get table-name->ns table) name))))}
                                opts)
          rsmeta         (.getMetaData rset)
          col-names      (next.jdbc.rs/get-modified-column-names rsmeta opts)]
      (log/debugf :results "column names: %s" col-names)
      (->InstanceBuilder model rset rsmeta col-names))))

(m/defmethod builder-fn :default
  [conn model]
  next.jdbc.rs/as-lower-maps
  (next.jdbc.rs/builder-adapter
   (instance-builder-fn model)
   (read-column-by-index-fn conn model)))


;;; A lot of the stuff below is an adapted/custom version of the code in [[next.jdbc.result-set]] -- I would have
;;; preferred to not have to do this but a lot of it was necessary to make things work in the Toucan 2 work. See this
;;; Slack thread for more information: https://clojurians.slack.com/archives/C1Q164V29/p1662494291800529

(defn- build-transient-row [builder]
  (reduce (fn [row i]
            (next.jdbc.rs/with-column builder row i))
          (next.jdbc.rs/->row builder)
          (range 1 (inc (next.jdbc.rs/column-count builder)))))

(defn- build-row
  [builder]
  (assert (not (.isClosed ^ResultSet (:rset builder))) "ResultSet is already closed")
  (next.jdbc.rs/row! builder (build-transient-row builder)))

(defn- make-current-row-thunk [^ResultSet rset]
  (let [current-row (atom 1)]
    (fn []
      (if (.isClosed rset)
        @current-row
        (let [row (.getRow rset)]
          (reset! current-row row)
          row)))))

(defn- make-i->thunk [conn model ^ResultSet rset builder]
  (let [current-row-thunk (make-current-row-thunk rset)
        i->thunk-delay    (into {}
                                (map (fn [i]
                                       [i (delay (let [f (read-column-by-index-fn conn model)]
                                                   (fn thunk* []
                                                     (f builder rset i))))]))
                                (range 1 (inc (count (:cols builder)))))
        i->uncached-thunk (fn [i]
                            (some-> (get i->thunk-delay i) deref))]
    (fn [i]
      (let [thunk        (i->uncached-thunk i)
            cached-row   (atom -1)
            cached-value (atom nil)]
        (fn []
          (let [current-row (current-row-thunk)]
            (if (= @cached-row current-row)
              (do
                (log/tracef :results "Using cached value for row %s column %s => %s" current-row i @cached-value)
                @cached-value)
              (let [v (thunk)]
                (reset! cached-row current-row)
                (reset! cached-value v)
                v))))))))

(defn- make-name->thunk [model builder i->thunk]
  (let [key-xform (instance/key-transform-fn model)
        name->i   (zipmap (:cols builder)
                          (range 1 (inc (count (:cols builder)))))]
    (fn [col-name]
      (let [col-name (key-xform col-name)]
        (when-let [i (get name->i (keyword col-name))]
          (i->thunk i))))))

(defn- make-row-thunk [rset builder]
  (let [cached-row-num    (atom -1)
        cached-row        (atom nil)
        current-row-thunk (make-current-row-thunk rset)]
    (fn []
      (let [current-row-num (current-row-thunk)]
        (if (= @cached-row-num current-row-num)
          (do
            (log/tracef :results "Using cached row builder for row %s" current-row-num)
            @cached-row)
          (let [row (build-row builder)]
            (reset! cached-row current-row-num)
            (reset! cached-row row)
            (assert (instance/instance? row) (format "Expected row to be an instance, got ^%s %s"
                                                     (some-> row class .getCanonicalName)
                                                     (pr-str row)))
            row))))))

(defprotocol ^:private MapifiedResultSet)

(defn- mapify-result-set
  "This is adapted from [[next.jdbc.result-set/mapify-result-set]] but has several important differences to make it
  Toucan-compatible."
  [^Connection conn model ^ResultSet rset opts]
  (let [rsmeta      (.getMetaData rset)
        builder     ((get opts :builder-fn next.jdbc.rs/as-maps) rset opts)
        i->thunk    (make-i->thunk conn model rset builder)
        name->thunk (make-name->thunk model builder i->thunk)
        row         (make-row-thunk rset builder)]
    (reify
      ;; marker, just for printing resolution
      toucan2.jdbc.result_set.MapifiedResultSet

      next.jdbc.result_set.InspectableMapifiedResultSet
      (row-number   [_this] (.getRow rset))
      (column-names [_this] (:cols builder))
      (metadata     [_this] (d/datafy rsmeta))

      clojure.lang.IPersistentMap
      (assoc [_this k v]
        (log/tracef :results ".assoc %s %s" k v)
        (assoc (row) k v))
      (assocEx [_this k v]
        (log/tracef :results ".assocEx %s %s" k v)
        (.assocEx ^clojure.lang.IPersistentMap (row) k v))
      (without [_this k]
        (log/tracef :results ".without %s" k)
        (dissoc (row) k))

      java.lang.Iterable                ; Java 7 compatible: no forEach / spliterator
      (iterator [_this]
        (log/tracef :results ".iterator")
        (.iterator ^java.lang.Iterable (row)))

      clojure.lang.Associative
      (containsKey [_this k]
        (log/tracef :results ".containsKey %s" k)
        (try
          (.getObject rset (name k))
          true
          (catch java.sql.SQLException _
            false)))
      (entryAt [_this k]
        (log/tracef :results ".entryAt %s" k)
        (when-let [thunk (name->thunk k)]
          (try
            (clojure.lang.MapEntry. k (thunk))
            (catch java.sql.SQLException _))))

      clojure.lang.Counted
      (count [_this]
        (log/tracef :results ".count")
        (next.jdbc.rs/column-count builder))

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
        {})
      (equiv [_this obj]
        (log/tracef :results ".equiv %s" obj)
        (.equiv ^clojure.lang.IPersistentCollection (row) obj))

      ;; we support get with a numeric key for array-based builders:
      clojure.lang.ILookup
      (valAt [this k]
        (log/tracef :results ".valAt %s" k)
        (.valAt this k nil))
      (valAt [_this k not-found]
        (log/tracef :results ".valAt %s %s" k not-found)
        (try
          (if-let [thunk (if (number? k)
                           (let [i (inc k)]
                             (i->thunk i))
                           (name->thunk k))]
            (thunk)
            not-found)
          (catch java.sql.SQLException _
            not-found)))

      ;; we support nth for array-based builderset (i is primitive int here!):
      clojure.lang.Indexed
      (nth [_this i]
        (log/tracef :results ".nth %s" i)
        (try
          (let [i (inc i)]
            (next.jdbc.rs/read-column-by-index (.getObject rset i) (:rsmeta builder) i))
          (catch java.sql.SQLException _)))
      (nth [_this i not-found]
        (log/tracef :results ".nth %s %s" i not-found)
        (try
          (let [i (inc i)]
            (next.jdbc.rs/read-column-by-index (.getObject rset i) (:rsmeta builder) i))
          (catch java.sql.SQLException _
            not-found)))

      clojure.lang.Seqable
      (seq [_this]
        (log/tracef :results ".seq")
        (seq (row)))

      next.jdbc.rs/DatafiableRow
      (datafiable-row [_this connectable opts]
        ;; since we have to call these eagerly, we trap any exceptions so
        ;; that they can be thrown when the actual functions are called
        (let [row   (try (.getRow rset)  (catch Throwable t t))
              cols  (try (:cols builder) (catch Throwable t t))
              metta (try (d/datafy rsmeta) (catch Throwable t t))]
          (vary-meta
           (row)
           assoc
           `core-p/datafy (#'next.jdbc.rs/navize-row connectable opts)
           `core-p/nav    (#'next.jdbc.rs/navable-row connectable opts)
           `row-number    (fn [_this] (if (instance? Throwable row) (throw row) row))
           `column-names  (fn [_this] (if (instance? Throwable cols) (throw cols) cols))
           `metadata      (fn [_this] (if (instance? Throwable metta) (throw metta) metta)))))

      protocols/IModel
      (model [_this]
        model)

      realize/Realize
      (realize [_this]
        (row))

      (toString [_this]
        (try
          (str (row))
          (catch Throwable _
            "{row} from `plan` -- missing `map` or `reduce`?"))))))

(def ^:private print-symbol (symbol (str "^" `mapify-result-set$reify " ")))

(defmethod print-method toucan2.jdbc.result_set.MapifiedResultSet
  [mrs writer]
  (print-method print-symbol writer)
  (print-method (into {} mrs) writer))

(defmethod pprint/simple-dispatch toucan2.jdbc.result_set.MapifiedResultSet
  [mrs]
  (pprint/simple-dispatch print-symbol)
  (pprint/simple-dispatch (into {} mrs)))

(prefer-method print-method toucan2.jdbc.result_set.MapifiedResultSet clojure.lang.IPersistentMap)
(prefer-method pprint/simple-dispatch toucan2.jdbc.result_set.MapifiedResultSet clojure.lang.IPersistentMap)

(defn reduce-result-set [rf init conn model ^ResultSet rset opts]
  (log/debugf :execute "Reduce JDBC result set for model %s with rf %s and init %s" model rf init)
  (let [opts   (merge {:builder-fn (builder-fn conn model)} opts)
        rs-map (mapify-result-set conn model rset opts)]
    (loop [acc init]
      (if (.next rset)
        (let [result (rf acc rs-map)]
          (if (reduced? result)
            @result
            (recur result)))
        acc))))

;;;; Postgres integration

;;; TODO -- why is cljdoc picking this up?
(when-let [pg-connection-class (try
                                 (Class/forName "org.postgresql.jdbc.PgConnection")
                                 (catch Throwable _
                                   nil))]
  (m/defmethod read-column-thunk [pg-connection-class :default Types/TIMESTAMP]
    [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
    (let [^Class klass (if (= (.getColumnTypeName rsmeta i) "timestamptz")
                         java.time.OffsetDateTime
                         java.time.LocalDateTime)]
      (get-object-of-class-thunk rset i klass))))
