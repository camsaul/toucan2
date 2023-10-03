(ns toucan2.jdbc.postgres
  "PostgreSQL integration."
  (:require
   [methodical.core :as m]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.util :as u])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

(when-let [pg-connection-class (try
                                 (Class/forName "org.postgresql.jdbc.PgConnection")
                                 (catch Throwable _
                                   nil))]
  (derive pg-connection-class ::connection))

(m/defmethod jdbc.read/read-column-thunk [#_conn  ::connection
                                          #_model :default
                                          #_type  Types/TIMESTAMP]
  "Both Postgres `timestamp` and `timestamp with time zone` come back as `java.sql.Types/TIMESTAMP`; check the actual
  database column type name so we can fetch objects as the correct class."
  [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
  (let [^Class klass (if (= (u/lower-case-en (.getColumnTypeName rsmeta i)) "timestamptz")
                       java.time.OffsetDateTime
                       java.time.LocalDateTime)]
    (jdbc.read/get-object-of-class-thunk rset i klass)))

(m/prefer-method! #'jdbc.read/read-column-thunk
                  [::connection :default Types/TIMESTAMP]
                  [java.sql.Connection :default Types/TIMESTAMP])
