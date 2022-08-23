(ns toucan2.jdbc.result-set
  (:require
   [methodical.core :as m]
   [next.jdbc.result-set :as jdbc.rs]
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan2.instance :as instance]
   [toucan2.jdbc.row :as row]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u])
  (:import
   (java.sql Connection ResultSet ResultSetMetaData Types)))

(def type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

    (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\""
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^Connection conn model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i])}
  (fn [^Connection conn model _rset ^ResultSetMetaData rsmeta ^Long i]
    (let [col-type (.getColumnType rsmeta i)]
      (u/println-debug ["Column %s %s is of JDBC type %s, native type %s"
                        i
                        (.getColumnLabel rsmeta i)
                        (type-name col-type)
                        (.getColumnTypeName rsmeta i)])
      [(protocols/dispatch-value conn) (protocols/dispatch-value model) col-type])))

(m/defmethod read-column-thunk :default
  [_conn _model ^ResultSet rset _rsmeta ^Long i]
  (u/println-debug ["Fetching values in column %s with %s" i (list '.getObject 'rs i)])
  (fn default-read-column-thunk []
    (.getObject rset i)))

(defn get-object-of-class-thunk [^ResultSet rset ^Long i ^Class klass]
  (u/println-debug ["Fetching values in column %s with %s"
                    i
                    (list '.getObject 'rs i (symbol (.getCanonicalName klass)))])
  (fn get-object-of-class-thunk []
    (.getObject rset i klass)))

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn- row-instance [model #_key-xform col-name->thunk]
  (let [row (row/row col-name->thunk)]
    (u/with-debug-result ["Creating new instance of %s, which has key transform fn %s"
                          model
                          (instance/key-transform-fn model)]
      (instance/instance model row))))

(defn row-thunk
  "Return a thunk that when called fetched the current row from the cursor and returns it as a [[row-instance]]."
  [^Connection conn model ^ResultSet rset]
  (let [rsmeta          (.getMetaData rset)
        key-xform       (instance/key-transform-fn model)
        ;; create a set of thunks to read each column. These thunks will call `read-column-thunk` to determine the
        ;; appropriate column-reading thunk the first time they are used.
        col-name->thunk (into {} (for [^Long i (index-range rsmeta)
                                       :let    [col-name     (key-xform (keyword (.getColumnName rsmeta i)))
                                                ;; TODO -- add test to ensure we only resolve the read-column-thunk
                                                ;; once even with multiple rows.
                                                read-thunk   (delay (read-column-thunk conn model rset rsmeta i))
                                                result-thunk (fn []
                                                               (u/with-debug-result ["Realize column %s %s" i col-name]
                                                                 (jdbc.rs/read-column-by-index (@read-thunk) rsmeta i)))]]
                                   [col-name result-thunk]))]
    (fn row-instance-thunk []
      (row-instance model #_key-xform col-name->thunk))))

(p/deftype+ ReducibleResultSet [^Connection conn model ^ResultSet rset]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (try
      (let [thunk (row-thunk conn model rset)]
        (loop [acc init]
          (cond
            (reduced? acc)
            @acc

            (.next rset)
            (recur (rf acc (thunk)))

            :else
            acc)))
      (catch Throwable e
        (throw (ex-info (format "Error reducing results: %s" (ex-message e))
                        {:rf rf, :init init, :result-set rset}
                        e)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-result-set conn model rset)))

(defn reducible-result-set [^Connection conn model rset]
  (->ReducibleResultSet conn model rset))

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

;;;; Postgres integration

(when-let [pg-connection-class (try
                                 (Class/forName "org.postgresql.jdbc.PgConnection")
                                 (catch Throwable _
                                   nil))]
  (m/defmethod read-column-thunk [pg-connection-class :default Types/TIMESTAMP]
    [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
    (let [^Class klass (if (= (u/lower-case-en (.getColumnTypeName rsmeta i)) "timestamptz")
                         java.time.OffsetDateTime
                         java.time.LocalDateTime)]
      (get-object-of-class-thunk rset i klass))))

(when-let [c3p0-connection-class (try
                                   (Class/forName "com.mchange.v2.c3p0.impl.NewProxyConnection")
                                   (catch Throwable _
                                     nil))]
  (extend c3p0-connection-class
    protocols/DispatchValue
    {:dispatch-value (fn [^java.sql.Wrapper conn]
                       (try
                         (protocols/dispatch-value (.unwrap conn java.sql.Connection))
                         (catch Throwable _
                           c3p0-connection-class)))}))
