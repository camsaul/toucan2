(ns toucan2.jdbc.query
  (:require
   [next.jdbc :as next.jdbc]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(def global-options
  "Default options automatically passed to all [[next.jdbc]] queries."
  (atom nil))

(def ^:dynamic *options*
  "Options to pass to [[next.jdbc]] when executing queries or statements."
  nil)

(defn- options []
  (merge @global-options *options*))

(defn reduce-jdbc-query [^java.sql.Connection conn model sql-args rf init extra-options]
  {:pre [(instance? java.sql.Connection conn) (sequential? sql-args) (string? (first sql-args)) (ifn? rf)]}
  (let [opts (merge (options) extra-options)]
    (log/debugf :execute "Preparing JDBC query with next.jdbc options %s" opts)
    (u/try-with-error-context [(format "execute SQL with %s" (class conn)) {::sql-args sql-args}]
      (with-open [stmt (next.jdbc/prepare conn sql-args opts)]
        (log/tracef :execute "Executing statement with %s" (class conn))
        (let [result-set? (.execute stmt)]
          (cond
            (:return-keys opts)
            (do
              (log/debugf :execute "Query was executed with %s; returning generated keys" :return-keys)
              (with-open [rset (.getGeneratedKeys stmt)]
                (reduce rf init (jdbc.rs/reducible-result-set conn model rset))))

            result-set?
            (with-open [rset (.getResultSet stmt)]
              (reduce rf init (jdbc.rs/reducible-result-set conn model rset)))

            :else
            (do
              (log/debugf :execute "Query did not return a ResultSet; nothing to reduce. Returning update count.")
              (reduce rf init [(.getUpdateCount stmt)]))))))))
