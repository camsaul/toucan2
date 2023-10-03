(ns toucan2.jdbc.connection
  (:require
   [methodical.core :as m]
   [next.jdbc]
   [next.jdbc.transaction]
   [toucan2.connection :as conn]
   [toucan2.log :as log]))

(set! *warn-on-reflection* true)

(m/defmethod conn/do-with-connection java.sql.Connection
  [conn f]
  (f conn))

(m/defmethod conn/do-with-connection javax.sql.DataSource
  [^javax.sql.DataSource data-source f]
  (with-open [conn (.getConnection data-source)]
    (f conn)))

(m/defmethod conn/do-with-connection clojure.lang.IPersistentMap
  "Implementation for map connectables. Treats them as a `clojure.java.jdbc`-style connection spec map, converting them to
  a `java.sql.DataSource` with [[next.jdbc/get-datasource]]."
  [m f]
  (conn/do-with-connection (next.jdbc/get-datasource m) f))

;;; for record types that implement `DataSource`, prefer the `DataSource` impl over the map impl.
(m/prefer-method! #'conn/do-with-connection javax.sql.DataSource clojure.lang.IPersistentMap)

(m/defmethod conn/do-with-connection-string "jdbc"
  "Implementation of `do-with-connection-string` (and thus [[do-with-connection]]) for all strings starting with `jdbc:`.
  Calls `java.sql.DriverManager/getConnection` on the connection string."
  [^String connection-string f]
  (with-open [conn (java.sql.DriverManager/getConnection connection-string)]
    (f conn)))

(m/defmethod conn/do-with-transaction java.sql.Connection
  [^java.sql.Connection conn options f]
  (let [nested-tx-rule (get options :nested-transaction-rule next.jdbc.transaction/*nested-tx*)
        options        (dissoc options :nested-transaction-rule)]
    (log/debugf "do with JDBC transaction (nested rule: %s) with options %s" nested-tx-rule options)
    (binding [next.jdbc.transaction/*nested-tx* nested-tx-rule]
      (next.jdbc/with-transaction [t-conn conn options]
        (f t-conn)))))
