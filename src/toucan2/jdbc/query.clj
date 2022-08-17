(ns toucan2.jdbc.query
  (:require
   [next.jdbc :as jdbc]
   [toucan2.jdbc.result-set :as t2.jdbc.rs]
   [toucan2.util :as u]))

(def global-options
  "Default options automatically passed to all [[next.jdbc]] queries."
  (atom nil))

(def ^:dynamic *options*
  "Options to pass to [[next.jdbc]] when executing queries or statements."
  nil)

(defn- options []
  (merge @global-options *options*))

(defn reduce-jdbc-query [^java.sql.Connection conn model sql-args rf init]
  (let [opts (options)]
    (u/println-debug ["Preparing JDBC query with next.jdbc options %s" opts])
    (with-open [stmt (jdbc/prepare conn sql-args opts)]
      (u/println-debug "Executing statement")
      (let [result-set? (.execute stmt)]
        (cond
          (:return-keys opts)
          (do
            (u/println-debug ["Query was executed with %s; returning generated keys" :return-keys])
            (with-open [rset (.getGeneratedKeys stmt)]
              (reduce rf init (t2.jdbc.rs/reducible-result-set conn model rset))))

          result-set?
          (with-open [rset (.getResultSet stmt)]
            (reduce rf init (t2.jdbc.rs/reducible-result-set conn model rset)))

          :else
          (do
            (u/println-debug "Query did not return a ResultSet; nothing to reduce. Returning update count.")
            (reduce rf init [(.getUpdateCount stmt)])
            #_init
            #_(reduce rf init nil)))))))

;; (defn execute-jdbc-query! [^java.sql.Connection conn sql-args]
;;   (let [opts (options)]
;;     (u/with-debug-result (format "Executing JDBC query with options %s" (u/pretty-print opts))
;;       (jdbc/execute! conn sql-args opts))))
