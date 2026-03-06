(ns toucan2.jdbc.sqlite
  "SQLite integration."
  (:require
   [methodical.core :as m]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

(doseq [^String connection-class-name ["org.sqlite.SQLiteConnection"
                                        "org.sqlite.jdbc3.JDBC3Connection"]]
  (when-let [connection-class (try
                                (Class/forName connection-class-name)
                                (catch Throwable _))]
    (derive connection-class ::connection)))

(m/defmethod pipeline/transduce-execute-with-connection :around [#_conn       ::connection
                                                                  #_query-type :default
                                                                  #_model      :default]
  "Enable foreign keys for SQLite connections."
  [rf conn query-type model compiled-query]
  (with-open [stmt (.createStatement ^java.sql.Connection conn)]
    (.execute stmt "PRAGMA foreign_keys = ON"))
  (next-method rf conn query-type model compiled-query))
