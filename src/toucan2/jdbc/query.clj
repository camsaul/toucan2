(ns ^:no-doc toucan2.jdbc.query
  (:require
   [next.jdbc :as next.jdbc]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.util :as u])
  (:import java.sql.ResultSet))

(set! *warn-on-reflection* true)

;;; TODO: it's a little silly having a one-function namespace. Maybe we should move this into one other ones

(def ^:private read-forward-options
  "We normally only read in a forward direction, and treat result sets as read-only. So make sure the JDBC can optimize
  things when possible. Note that you're apparently not allowed to do this when `:return-keys` is set. So this only is
  merged in otherwise."
  {:concurrency :read-only
   :cursors     :close
   :result-type :forward-only})

(defn ^:no-doc reduce-jdbc-query
  "Execute `sql-args` against a JDBC connection `conn`, and reduce results with reducing function `rf` and initial value
  `init`. Part of the implementation of the JDBC backend; you shouldn't need to call this directly."
  [rf init ^java.sql.Connection conn model sql-args extra-options]
  {:pre [(instance? java.sql.Connection conn) (sequential? sql-args) (string? (first sql-args)) (ifn? rf)]}
  (let [opts (jdbc.options/merge-options extra-options)
        opts (merge (when-not (:return-keys opts)
                      read-forward-options)
                    opts)]
    (log/debugf "Preparing JDBC query with next.jdbc options %s" opts)
    (u/try-with-error-context [(format "execute SQL with %s" (class conn)) {::sql-args sql-args}]
      (with-open [stmt (next.jdbc/prepare conn sql-args opts)]
        (when-not (= (.getFetchDirection stmt) ResultSet/FETCH_FORWARD)
          (try
            (.setFetchDirection stmt ResultSet/FETCH_FORWARD)
            (catch Throwable e
              (log/debugf e "Error setting fetch direction"))))
        (log/tracef "Executing statement with %s" (class conn))
        (let [result-set? (.execute stmt)]
          (cond
            (:return-keys opts)
            (do
              (log/debugf "Query was executed with %s; returning generated keys" :return-keys)
              (with-open [rset (.getGeneratedKeys stmt)]
                (jdbc.rs/reduce-result-set rf init conn model rset opts)))

            result-set?
            (with-open [rset (.getResultSet stmt)]
              (log/debugf "Query returned normal result set")
              (jdbc.rs/reduce-result-set rf init conn model rset opts))

            :else
            (do
              (log/debugf "Query did not return a ResultSet; nothing to reduce. Returning update count.")
              (reduce rf init [(.getUpdateCount stmt)]))))))))
