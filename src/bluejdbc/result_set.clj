(ns bluejdbc.result-set
  (:require [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [clojure.string :as str]
            [methodical.core :as m])
  (:import [java.sql ResultSet ResultSetMetaData Types]))

;;;; Reading Results

;; TODO -- should this be exposed in `core`?
(def type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

    (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\""
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmeta ^Integer i options])}
  (fn [rs ^ResultSetMetaData rsmeta ^Integer i options]
    ;; TODO - log nice name for col type
    (let [col-type (.getColumnType rsmeta i)]
      (log/tracef "Column %d %s is of JDBC type %s, native type %s"
                  i (pr-str (.getColumnLabel rsmeta i)) (type-name col-type) (.getColumnTypeName rsmeta i))
      [(:connection/type options) col-type])))

(m/defmethod read-column-thunk :default
  [^ResultSet rs rsmeta ^Integer i options]
  (fn default-read-column-thunk []
    (.getObject rs i)))

(m/defmethod read-column-thunk :around :default
  [rs rsmeta i options]
  (log/tracef "Fetching values in column %d with (.getObject rs %d) and options %s" i i (pr-str options))
  (next-method rs rsmeta i options))

(defn get-object-of-class-thunk [^ResultSet rs ^Integer i ^Class klass]
  (log/tracef "Fetching values in column %d with (.getObject rs %d %s)" i i (.getCanonicalName klass))
  (fn get-object-of-class-thunk* []
    (.getObject rs i klass)))

(m/defmethod read-column-thunk [:default Types/TIMESTAMP]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default Types/TIMESTAMP_WITH_TIMEZONE]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetDateTime))

(m/defmethod read-column-thunk [:default Types/DATE]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDate))

(m/defmethod read-column-thunk [:default Types/TIME]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalTime))

(m/defmethod read-column-thunk [:default Types/TIME_WITH_TIMEZONE]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetTime))

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn row-thunk
  "Returns a thunk that can be called repeatedly to get the next row in the result set, using appropriate methods to
  fetch each value in the row. Returns `nil` when the result set has no more rows."
  ([rs]
   (row-thunk rs nil))

  ([^ResultSet rs options]
   (let [rsmeta (.getMetaData rs)
         fns    (for [i (index-range rsmeta)]
                  (read-column-thunk rs rsmeta (int i) options))]
     (if (empty? fns)
       (constantly nil)
       (let [thunk (apply juxt fns)]
         (fn row-thunk* []
           (when (.next rs)
             (thunk))))))))


;;;; row xforms

(defn namespaced-column-names
  "Return a sequence of namespaced keyword column names for the rows in a `ResultSet`, e.g. `:table/column`."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getTableName rsmeta i) (.getColumnLabel rsmeta i))))))

(defn column-names
  "Return a sequence of column names (as keywords) for the rows in a `ResultSet`."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getColumnLabel rsmeta i))))))

(m/defmulti transform-column-names
  "Impl for the `maps` result set rows transform. See docstring for `maps` for more details."
  {:arglists '([transform-name column-names])}
  (fn [transform-name _]
    (keyword transform-name)))

(m/defmethod transform-column-names :lower-case
  [_ names]
  (mapv (comp keyword str/lower-case u/qualified-name)
        names))

(m/defmethod transform-column-names :upper-case
  [_ names]
  (mapv (comp keyword str/upper-case u/qualified-name)
        names))

(m/defmethod transform-column-names :lisp-case
  [_ names]
  (mapv (comp keyword #(str/replace % #"_" "-") u/qualified-name)
        names))

(m/defmethod transform-column-names :snake-case
  [_ names]
  (mapv (comp keyword #(str/replace % #"-" "_") u/qualified-name)
        names))

(defn column-names-with-options
  "Implementations of the `maps` `ResultSet` transform. Fetch a sequence column names from `ResultSet` `rs` and apply
  transforms in `options-set`. `options-set` can include any method of `transform-column-names`.

    (column-names-with-options rs nil)           ; -> [:col_1 :col_2]
    (column-names-with-options rs #{:lisp-case}) ; -> [:col-1 :col-2]"
  [rs options-set]
  (let [names (if (contains? options-set :namespaced)
                (namespaced-column-names rs)
                (column-names rs))]
    (reduce
     (fn [names option]
       (transform-column-names option names))
     names
     (disj options-set :namespaced))))

(defn- maps-reducing-fn [rs options]
  (let [column-names (column-names-with-options rs options)]
    (fn [rf]
      (fn
        ([]
         (rf))

        ([acc]
         (rf acc))

        ([acc row]
         (rf acc (zipmap column-names row)))))))

(defn- maps*
  [default-options]
  (fn [x & more]
    (if (instance? ResultSet x)
      (let [rs x]
        (maps-reducing-fn rs default-options))
      (let [options (into (set default-options) (cons x more))]
        (fn [rs]
          (maps-reducing-fn rs options))))))

(def ^{:arglists '([rs] [& options])} maps
  "A `ResultSet` transform that returns rows as maps. By default, maps are unqualified keys, e.g. `:column`; this is the
  default row transform used if no other values of `:results/xform` are passed in options.

  `maps` can take one or more `options` that transform the keys used in the resulting maps:

    (jdbc/query conn \"SELECT * FROM user\" {:results/xform (maps :namespaced)})
    ;; -> [{:user/id 1, :user/name \"Cam\"} ...]

  Current options include `:namespaced`, `:lisp-case`, `:snake-case`, `:lower-case`, and `:upper-case`. You can add
  more options by adding implementations for `bluejdbc.result-set/transform-column-names`."
  (maps* nil))

;;;; Reducing/seq-ing result sets

(defn reduce-result-set
  "Reduce the rows in a `ResultSet` using reducing function `rf`."
  {:arglists '([rs rf init] [rs options rf init])}
  ([rs rf init]
   (reduce-result-set rs nil rf init))

  ([rs {xform :results/xform, :or {xform maps}, :as options} rf init]
   (let [xform (comp (take-while some?)
                     (if xform
                       (xform rs)
                       identity))
         thunk (row-thunk rs options)]
     (reduce
      (xform rf)
      init
      (repeatedly thunk)))))

(defn reducible-result-set ^clojure.lang.IReduceInit [^ResultSet rs options]
  (reify
    clojure.lang.IReduceInit
    (reduce [_ rf init]
      (reduce-result-set rs options rf init))))

(defn result-set-seq
  "Return a lazy sequence of rows from a `ResultSet`. Make sure you consume all the rows in the `ResultSet` while it is
  still open!

    ;; good
    (with-open [rs (jdbc/results stmt)]
      (doseq [row (result-set-seq rs)]
        ...))

    ;; bad -- ResultSet is closed before we finish the `doseq` loop
    (doseq [row (with-open [rs (jdbc/results stmt)]
                  (jdbc/results stmt))]
      ...)"
  {:arglists '(^clojure.lang.ISeq [rs]
               ^clojure.lang.ISeq [rs options])}
  ([rs]
   (result-set-seq rs nil))

  ([rs {xform :results/xform, :or {xform maps}}]
   (let [xform        (if xform
                        (xform rs)
                        identity)
         rf           (xform (fn [_ row] row))
         thunk        (row-thunk rs)
         ;; TODO -- should this be `^:once fn*` ??
         lazy-results (fn lazy-results []
                        (lazy-seq
                         (when-let [row (thunk)]
                           (cons (rf nil row) (lazy-results)))))]
     (lazy-results))))
