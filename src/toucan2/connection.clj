(ns toucan2.connection
  (:require [methodical.core :as m]
            [toucan2.util :as u]))

(m/defmulti do-with-connection
  {:arglists '([connectable f])}
  u/dispatch-on-keyword-or-type-1)

(defmacro with-connection [[connection-binding connectable] & body]
  `(do-with-connection
    ~connectable
    (^:once fn* [~connection-binding] ~@body)))

(m/defmethod do-with-connection java.sql.Connection
  [conn f]
  (f conn))

(m/defmethod do-with-connection javax.sql.DataSource
  [^javax.sql.DataSource data-source f]
  (with-open [conn (.getConnection data-source)]
    (f conn)))

(m/defmulti do-with-connection-string
  {:arglists '([^java.lang.String protocol ^java.lang.String connection-string f])}
  (fn [protocol _connection-string _f]
    protocol))

(defn connection-string-protocol [connection-string]
  (when connection-string
    (second (re-find #"^(?:([^:]+):)" connection-string))))

(m/defmethod do-with-connection String
  [connection-string f]
  (do-with-connection-string (connection-string-protocol connection-string)
                             connection-string
                             f))

(m/defmethod do-with-connection-string "jdbc"
  [_protocol ^String connection-string f]
  (with-open [conn (java.sql.DriverManager/getConnection connection-string)]
    (f conn)))
