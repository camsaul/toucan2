(ns toucan2.connection
  (:require
   [methodical.core :as m]
   [toucan2.util :as u]
   [next.jdbc :as jdbc]
   [toucan2.current :as current]))

(m/defmulti do-with-connection
  {:arglists '([connectable f])}
  u/dispatch-on-first-arg)

;; TODO -- don't love this syntax.
(defmacro with-connection
  {:arglists '([[connection-binding connectable] & body]
               [[connection-binding connectable] & body])}
  [[connection-binding connectable] & body]
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

(m/defmethod do-with-connection clojure.lang.IPersistentMap
  [m f]
  (do-with-connection (jdbc/get-datasource m)
                      (^:once fn* [conn]
                       (binding [current/*connection* conn]
                         (f conn)))))

;;;; connection string support

(defn connection-string-protocol [connection-string]
  (when connection-string
    (second (re-find #"^(?:([^:]+):)" connection-string))))

(m/defmulti do-with-connection-string
  {:arglists '([^java.lang.String connection-string f])}
  (fn [connection-string _f]
    (connection-string-protocol connection-string)))

(m/defmethod do-with-connection String
  [connection-string f]
  (do-with-connection-string connection-string f))

(m/defmethod do-with-connection-string "jdbc"
  [^String connection-string f]
  (with-open [conn (java.sql.DriverManager/getConnection connection-string)]
    (f conn)))
