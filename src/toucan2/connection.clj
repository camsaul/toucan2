(ns toucan2.connection
  (:require
   [methodical.core :as m]
   [toucan2.util :as u]
   [next.jdbc :as jdbc]))

(m/defmulti do-with-connection
  {:arglists '([connectable options f])}
  u/dispatch-on-keyword-or-type-1)

;; TODO -- don't love this syntax.
(defmacro with-connection
  {:arglists '([[connection-binding connectable] & body]
               [[connection-binding connectable options] & body])}
  [[connection-binding connectable options] & body]
  `(do-with-connection
    ~connectable
    ~options
    (^:once fn* [~connection-binding] ~@body)))

(m/defmethod do-with-connection java.sql.Connection
  [conn _options f]
  (f conn))

(m/defmethod do-with-connection javax.sql.DataSource
  [^javax.sql.DataSource data-source _options f]
  (with-open [conn (.getConnection data-source)]
    (f conn)))

(m/defmethod do-with-connection clojure.lang.IPersistentMap
  [m options f]
  (do-with-connection (jdbc/get-datasource m) options f))

;;;; connection string support

(defn connection-string-protocol [connection-string]
  (when connection-string
    (second (re-find #"^(?:([^:]+):)" connection-string))))

(m/defmulti do-with-connection-string
  {:arglists '([^java.lang.String connection-string options f])}
  (fn [connection-string _options _f]
    (connection-string-protocol connection-string)))

(m/defmethod do-with-connection String
  [connection-string options f]
  (do-with-connection-string connection-string options f))

(m/defmethod do-with-connection-string "jdbc"
  [^String connection-string _options f]
  (with-open [conn (java.sql.DriverManager/getConnection connection-string)]
    (f conn)))
