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

;;; c3p0 and Hikari integration, or any other library that wraps real SQL connections: when we encounter a wrapped
;;; connection, dispatch off of the class of connection it wraps
(extend java.sql.Connection
  protocols/IDispatchValue
  {:dispatch-value (fn [^java.sql.Wrapper conn]
                     (try
                       (type (.unwrap conn java.sql.Connection))
                       (catch Throwable _
                         (type conn))))})
