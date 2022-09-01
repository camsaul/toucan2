(ns toucan2.jdbc.result-set
  (:require
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [pretty.core :as pretty]
   [toucan2.instance :as instance]
   [toucan2.jdbc.row :as jdbc.row]
   [toucan2.log :as log]
   [toucan2.magic-map :as magic-map]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
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
                  (type-name col-type)
                  (.getColumnTypeName rsmeta i))
      [(protocols/dispatch-value conn) (protocols/dispatch-value model) col-type])))

(m/defmethod read-column-thunk :default
  [_conn _model ^ResultSet rset _rsmeta ^Long i]
  (log/debugf :results "Fetching values in column %s with %s" i (list '.getObject 'rs i))
  (fn default-read-column-thunk []
    (.getObject rset i)))

(defn get-object-of-class-thunk [^ResultSet rset ^Long i ^Class klass]
  (log/debugf :results
              "Fetching values in column %s with %s"
              i
              (list '.getObject 'rs i klass))
  (fn get-object-of-class-thunk []
    (.getObject rset i klass)))

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn- row-instance [model col-name->thunk]
  (log/debugf :results
              "Creating new instance of %s, which has key transform fn %s"
              model
              (instance/key-transform-fn model))
  (instance/instance model (jdbc.row/row col-name->thunk)))

(defn row-thunk
  "Return a thunk that when called fetched the current row from the cursor and returns it as a [[row-instance]]."
  [^Connection conn model ^ResultSet rset]
  (let [rsmeta           (.getMetaData rset)
        ;; do case-insensitive lookup.
        table->namespace (some-> (model/table-name->namespace model) (magic-map/magic-map u/lower-case-en))
        key-xform        (instance/key-transform-fn model)
        ;; create a set of thunks to read each column. These thunks will call `read-column-thunk` to determine the
        ;; appropriate column-reading thunk the first time they are used.
        col-name->thunk  (into
                          {}
                          (map (fn [^Long i]
                                 (let [table-name    (.getTableName rsmeta i)
                                       col-name      (.getColumnName rsmeta i)
                                       table-ns-name (some-> (get table->namespace table-name) name)
                                       col-key       (key-xform (keyword table-ns-name col-name))
                                       ;; TODO -- add test to ensure we only resolve the read-column-thunk
                                       ;; once even with multiple rows.
                                       read-thunk   (delay (read-column-thunk conn model rset rsmeta i))
                                       result-thunk (fn []
                                                      (log/tracef :results "Realize column %s %s.%s as %s"
                                                                  i table-name col-name col-key)
                                                      (let [result (next.jdbc.rs/read-column-by-index (@read-thunk) rsmeta i)]
                                                        (log/tracef :results "=> %s" result)
                                                        result))]
                                   [col-key result-thunk])))
                          (index-range rsmeta))]
    (fn row-instance-thunk []
      (row-instance model col-name->thunk))))

(deftype ^:no-doc ReducibleResultSet [^Connection conn model ^ResultSet rset]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (u/try-with-error-context [(format "reduce %s" `ReducibleResultSet) {::model model, ::rf rf, ::init init}]
      (let [thunk (row-thunk conn model rset)]
        (loop [acc init]
          (cond
            (reduced? acc)
            @acc

            (.next rset)
            (recur (rf acc (thunk)))

            :else
            acc)))))

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

;;; TODO -- why is cljdoc picking this up?
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
