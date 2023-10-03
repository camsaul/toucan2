(ns toucan2.jdbc
  (:require
   [toucan2.jdbc.connection :as jdbc.conn]
   [toucan2.jdbc.pipeline :as jdbc.pipeline]
   [toucan2.protocols :as protocols]))

(comment jdbc.conn/keep-me
         jdbc.pipeline/keep-me)

(set! *warn-on-reflection* true)

;;;; load the miscellaneous integrations

(defn- class-for-name ^Class [^String class-name]
  (try
    (Class/forName class-name)
    (catch Throwable _)))

(when (class-for-name "org.postgresql.jdbc.PgConnection")
  (require 'toucan2.jdbc.postgres))

(when (some class-for-name ["org.mariadb.jdbc.Connection"
                           "org.mariadb.jdbc.MariaDbConnection"
                           "com.mysql.cj.MysqlConnection"])
  (require 'toucan2.jdbc.mysql-mariadb))

;;; c3p0 and Hikari integration: when we encounter a wrapped connection pool connection, dispatch off of the class of
;;; connection it wraps
(doseq [pool-connection-class-name ["com.mchange.v2.c3p0.impl.NewProxyConnection"
                                    "com.zaxxer.hikari.pool.HikariProxyConnection"]]
  (when-let [pool-connection-class (class-for-name pool-connection-class-name)]
    (extend pool-connection-class
      protocols/IDispatchValue
      {:dispatch-value (fn [^java.sql.Wrapper conn]
                         (try
                           (protocols/dispatch-value (.unwrap conn java.sql.Connection))
                           (catch Throwable _
                             pool-connection-class)))})))
