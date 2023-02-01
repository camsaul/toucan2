(ns toucan2.query-execution-backend.jdbc.mysql-mariadb
  "MySQL and MariaDB integration (mostly workarounds for broken stuff).

  See also [[toucan2.tools.update-returning-pks-workaround]]. TODO -- I think we should roll that into this namespace
  and find a way to enable it by default for MySQL/MariaDB."
  (:require
   [methodical.core :as m]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

;;; TODO -- need the MySQL class here too.

(when-let [mariadb-connection-class (try
                                      (Class/forName "org.mariadb.jdbc.MariaDbConnection")
                                      (catch Throwable _))]
  (derive mariadb-connection-class ::connection))

(when-let [mysql-connection-class (try
                                    (Class/forName "com.mysql.cj.MysqlConnection")
                                    (catch Throwable _))]
  (derive mysql-connection-class ::connection))

(m/defmethod jdbc.read/read-column-thunk [#_conn  ::connection
                                          #_model :default
                                          #_type  Types/TIMESTAMP]
  "MySQL/MariaDB `timestamp` is normalized to UTC, so return it as an `OffsetDateTime` rather than a `LocalDateTime`.
  `datetime` columns should be returned as `LocalDateTime`. Both `timestamp` and `datetime` seem to come back as
  `java.sql.Types/TIMESTAMP`, so check the actual database column type name so we can fetch objects as the correct
  class."
  [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
  (let [^Class klass (if (= (u/lower-case-en (.getColumnTypeName rsmeta i)) "timestamp")
                       java.time.OffsetDateTime
                       java.time.LocalDateTime)]
    (jdbc.read/get-object-of-class-thunk rset i klass)))

(m/prefer-method! #'jdbc.read/read-column-thunk
                  [::connection :default Types/TIMESTAMP]
                  [java.sql.Connection :default Types/TIMESTAMP])

(m/defmethod pipeline/transduce-execute-with-connection [#_conn       ::connection
                                                         #_query-type :toucan.query-type/insert.pks
                                                         #_model      :default]
  "Apparently `RETURN_GENERATED_KEYS` doesn't work for MySQL/MariaDB if:

  1. Values for the primary key are specified in the INSERT itself, *and*

  2. The primary key is not an integer.

  So to work around this we will look at the rows we're inserting: if every rows specifies the primary key
  column(s) (including `nil` values), we'll transduce those specified values rather than what JDBC returns.

  This seems like it won't work if these values were arbitrary Honey SQL expressions. I suppose we could work around
  THAT problem by running the primary key values thru another SELECT query... but that just seems like too much. I guess
  we can cross that bridge when we get there."
  [rf conn query-type model compiled-query]
  (let [rows                 (:rows pipeline/*parsed-args*)
        pks                  (model/primary-keys model)
        return-pks-directly? (and (seq rows)
                                  (every? (fn [row]
                                            (every? (fn [k]
                                                      (contains? row k))
                                                    pks))
                                          rows))]
    (if return-pks-directly?
      (do
        (pipeline/transduce-execute-with-connection (pipeline/default-rf :toucan.query-type/insert.update-count)
                                                    conn
                                                    :toucan.query-type/insert.update-count
                                                    model
                                                    compiled-query)
        (transduce
         (map (model/select-pks-fn model))
         rf
         rows))
      (next-method rf conn query-type model compiled-query))))

(m/prefer-method! #'pipeline/transduce-execute-with-connection
                  [::connection :toucan.query-type/insert.pks :default]
                  [java.sql.Connection :toucan.result-type/pks :default])
