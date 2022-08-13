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

;;; method if this is called with something we don't know how to handle
(m/defmethod do-with-connection :default
  [connectable _f]
  (throw (ex-info (format "Don't know how to get a connection from ^%s %s. Do you need to implement %s for %s?"
                          (some-> connectable class .getCanonicalName)
                          (pr-str connectable)
                          `do-with-connection
                          (u/dispatch-value connectable))
                  {:connectable connectable})))

(m/defmethod do-with-connection ::current
  [_connectable f]
  (do-with-connection current/*connection* f))

;;; method called with the default value of [[toucan2.current/*connection*]] if no value of `:toucan2/default` is
;;; defined.
(m/defmethod do-with-connection :toucan/default
  [_connectable _f]
  ;; TODO -- link to appropriate documentation page online in the error once we actually have dox.
  (throw (ex-info (format "No default Toucan connection defined. You can define one by implementing %s for :toucan/default. You can also implement %s for a model."
                          `do-with-connection
                          'toucan2.model/default-connectable)
                  {})))

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
