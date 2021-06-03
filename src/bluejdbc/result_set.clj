(ns bluejdbc.result-set
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.row :as row]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc.result-set :as next.jdbc.rs]
            [potemkin :as p]
            [pretty.core :as pretty])
  (:import [java.sql ResultSet ResultSetMetaData Types]))

(def type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

    (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\""
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

(m/defmulti read-column-thunk*
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([connectable tableable ^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i options])}
  (fn [connectable tableable _ ^ResultSetMetaData rsmeta ^Integer i _]
    (let [col-type (.getColumnType rsmeta i)]
      (log/tracef "Column %d %s is of JDBC type %s, native type %s"
                  i (pr-str (.getColumnLabel rsmeta i)) (type-name col-type) (.getColumnTypeName rsmeta i))
      ;; TODO -- enable this only if some `*log-connectable*` flag is set. (TBD)
      #_(log/tracef "Using read-column-thunk* impl for dispatch value %s"
                    [(u/dispatch-value connectable) (u/dispatch-value tableable) col-type])
      [(u/dispatch-value connectable) (u/dispatch-value tableable) col-type])))

(m/defmethod read-column-thunk* :default
  [_ _ ^ResultSet rs _ ^Integer i options]
  (log/tracef "Fetching values in column %d with (.getObject rs %d) and options %s" i i (pr-str options))
  (fn default-read-column-thunk* []
    (.getObject rs i)))

(defn get-object-of-class-thunk [^ResultSet rs ^Integer i ^Class klass]
  (log/tracef "Fetching values in column %d with (.getObject rs %d %s)" i i (.getCanonicalName klass))
  (u/pretty-printable-fn
   (fn []
     (list '.getObject 'rs i klass))
   (fn get-object-of-class-thunk []
     (.getObject rs i klass))))

(m/defmethod read-column-thunk* [:default :default Types/CLOB]
  [_ _ ^ResultSet rs _ ^Integer i _]
  (fn get-string-thunk []
    (.getString rs i)))

(m/defmethod read-column-thunk* [:default :default Types/TIMESTAMP]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDateTime))

(m/defmethod read-column-thunk* [:default :default Types/TIMESTAMP_WITH_TIMEZONE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetDateTime))

(m/defmethod read-column-thunk* [:default :default Types/DATE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDate))

(m/defmethod read-column-thunk* [:default :default Types/TIME]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalTime))

(m/defmethod read-column-thunk* [:default :default Types/TIME_WITH_TIMEZONE]
  [_ _ rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetTime))

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn- row-instance [connectable tableable key-xform col-name->thunk]
  (let [row (row/row col-name->thunk)]
    (instance/instance* connectable tableable row row key-xform nil)))

(defn row-thunk [connectable tableable ^java.sql.ResultSet rs options]
  (let [rsmeta          (.getMetaData rs)
        key-xform       (instance/key-transform-fn* connectable tableable)
        col-name->thunk (into {} (for [^Long i (index-range rsmeta)
                                       :let    [col-name (key-xform (keyword (.getColumnName rsmeta i)))
                                                thunk    (delay
                                                           (let [thunk (read-column-thunk* connectable tableable rs rsmeta i options)]
                                                             (fn []
                                                               (next.jdbc.rs/read-column-by-index (thunk) rsmeta i))))
                                                thunk    (u/pretty-printable-fn
                                                          (fn []
                                                            (symbol (format "#function[bluejdbc.result-set/row-thunk/col-thunk-%d-%s]" i (name col-name))))
                                                          (fn []
                                                            (log/tracef "Realize column %d %s" i col-name)
                                                            (@thunk)))]]
                                   [col-name thunk]))]
    (fn row-instance-thunk []
      (row-instance connectable tableable key-xform col-name->thunk))))

(p/deftype+ ReducibleResultSet [connectable tableable ^java.sql.ResultSet rs options]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (try
      (let [row-thunk (row-thunk connectable tableable rs options)]
        (loop [acc init]
          (cond
            (reduced? acc)
            (unreduced acc)

            (.next rs)
            (recur (rf acc (row-thunk)))

            :else
            acc)))
      (catch Throwable e
        (throw (ex-info (format "Error reducing results: %s" (ex-message e))
                        {:rf rf, :init init, :result-set rs, :options options}
                        e)))))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `reducible-result-set) connectable tableable rs options)))

(defn reducible-result-set [connectable tableable rs options]
  (->ReducibleResultSet connectable tableable rs options))
