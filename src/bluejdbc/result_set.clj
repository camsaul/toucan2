(ns bluejdbc.result-set
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc.result-set :as jdbc.rs])
  (:import [java.sql ResultSet ResultSetMetaData Types]))

(def type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

    (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\""
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([connectable tableable ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i options])}
  (fn [connectable tableable _ ^ResultSetMetaData rsmeta ^Integer i _]
    ;; TODO - log nice name for col type
    (let [col-type (.getColumnType rsmeta i)]
      (log/tracef "Column %d %s is of JDBC type %s, native type %s"
                  i (pr-str (.getColumnLabel rsmeta i)) (type-name col-type) (.getColumnTypeName rsmeta i))
      [connectable tableable col-type])))

(m/defmethod read-column-thunk :default
  [_ _ ^ResultSet rs _ ^Integer i options]
  (fn default-read-column-thunk []
    (.getObject rs i)))

(m/defmethod read-column-thunk :around :default
  [connectable tableable rs rsmeta i options]
  (log/tracef "Fetching values in column %d with (.getObject rs %d) and options %s" i i (pr-str options))
  (next-method connectable tableable rs rsmeta i options))

(defn get-object-of-class-thunk [^ResultSet rs ^Integer i ^Class klass]
  (log/tracef "Fetching values in column %d with (.getObject rs %d %s)" i i (.getCanonicalName klass))
  (fn []
    (.getObject rs i klass)))

(m/defmethod read-column-thunk [:default :default Types/CLOB]
  [_ _ ^ResultSet rs _ ^Integer i _]
  (fn []
    (.getString rs i)))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP_WITH_TIMEZONE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetDateTime))

(m/defmethod read-column-thunk [:default :default Types/DATE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDate))

(m/defmethod read-column-thunk [:default :default Types/TIME]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalTime))

(m/defmethod read-column-thunk [:default :default Types/TIME_WITH_TIMEZONE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetTime))

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn row-builder-fn [connectable tableable]
  (u/pretty-printable-fn
   #(list `row-builder-fn (if (keyword? connectable) connectable 'connectable) tableable)
   (fn [rs options]
     (let [^ResultSet rs rs
           rsmeta        (.getMetaData rs)
           cols          (jdbc.rs/get-unqualified-column-names rsmeta options)
           i->col-thunk  (into {} (for [i (index-range rsmeta)]
                                    [i (read-column-thunk connectable tableable rs rsmeta i options)]))]
       (reify
         jdbc.rs/RowBuilder
         (->row [this]
           (transient (instance/instance connectable tableable {})))
         (column-count [this]
           (count cols))
         (with-column [this row i]
           (let [col-thunk (get i->col-thunk i)]
             (jdbc.rs/with-column-value this row (nth cols (dec i)) (col-thunk))))
         (with-column-value [this row col v]
           (assoc! row col v))
         (row! [this row]
           (persistent! row))

         jdbc.rs/ResultSetBuilder
         (->rs [this]
           (transient []))
         (with-row [this mrs row]
           (conj! mrs row))
         (rs! [this mrs]
           (persistent! mrs)))))))
