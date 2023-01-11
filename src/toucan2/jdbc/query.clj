(ns toucan2.jdbc.query
  (:require
   [next.jdbc :as next.jdbc]
   [toucan2.jdbc :as jdbc]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(defn reduce-jdbc-query [rf init ^java.sql.Connection conn model sql-args extra-options]
  {:pre [(instance? java.sql.Connection conn) (sequential? sql-args) (string? (first sql-args)) (ifn? rf)]}
  (let [opts (jdbc/merge-options extra-options)]
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
                (jdbc.rs/reduce-result-set rf init conn model rset opts)))

            result-set?
            (with-open [rset (.getResultSet stmt)]
              (log/debugf :execute "Query returned normal result set")
              (jdbc.rs/reduce-result-set rf init conn model rset opts))

            :else
            (do
              (log/debugf :execute "Query did not return a ResultSet; nothing to reduce. Returning update count.")
              (reduce rf init [(.getUpdateCount stmt)]))))))))
