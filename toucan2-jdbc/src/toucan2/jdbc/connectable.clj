(ns toucan2.jdbc.connectable
  (:require [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.transaction :as next.jdbc.transaction]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.util :as u]))

(derive java.sql.Connection :toucan2/jdbc)

(m/defmethod conn/connection* java.sql.Connection
  [conn options]
  {:connection  conn
   :new?        true
   :options     options})

(derive javax.sql.DataSource :toucan2/jdbc)

(m/defmethod conn/connection* :toucan2/jdbc
  [connectable options]
  {:connection  (next.jdbc/get-connection (u/unwrap-dispatch-on connectable))
   :new?        true
   :options     options})

(m/defmethod conn/do-with-transaction* :toucan2/jdbc
  [connectable options f]
  (conn/with-connection [conn connectable options]
    ;; "ignore" nested transactions -- we'll make it work ourselves.
    (binding [next.jdbc.transaction/*nested-tx* :ignore]
      (next.jdbc/with-transaction [tx-connection conn]
        (let [save-point (.setSavepoint tx-connection)]
          (try
            (binding [conn.current/*current-connection* tx-connection]
              (f conn.current/*current-connection*))
            (catch Throwable e
              (.rollback tx-connection save-point)
              (throw e))))))))
