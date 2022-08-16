(ns toucan2.connection
  (:require
   [methodical.core :as m]
   [toucan2.util :as u]
   [next.jdbc :as jdbc]
   [toucan2.current :as current]))

(set! *warn-on-reflection* true)

;;; TODO -- shouldn't

(m/defmulti do-with-connection
  {:arglists '([connectable f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-connection :around :default
  [connectable f]
  (next-method connectable (^:once fn* [conn]
                            (binding [current/*connection* conn]
                              (f conn)))))

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

(m/defmethod do-with-connection nil
  [_connectable f]
  (do-with-connection ::current f))

;; the difference between this and using [[current/*connection*]] directly is that this waits until it gets resolved by
;; [[do-with-connection]] to get the value for [[current/*connection*]]. For a reducible query this means you'll get the
;; value at the time you reduce the query rather than at the time you build the reducible query.
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
  (do-with-connection (jdbc/get-datasource m) f))

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

(m/defmulti do-with-transaction
  {:arglists '([connection f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-transaction :around :default
  [connection f]
  (u/with-debug-result [(list `do-with-transaction (some-> connection class .getCanonicalName symbol))]
    (next-method connection (^:once fn* [conn]
                             (binding [current/*connection* conn]
                               (f conn))))))

(m/defmethod do-with-transaction java.sql.Connection
  [^java.sql.Connection conn f]
  (jdbc/with-transaction [t-conn conn]
    (f t-conn)))

(defmacro with-transaction
  {:style/indent 1}
  [[conn-binding connectable] & body]
  `(with-connection [conn# ~connectable]
     (do-with-transaction conn#
                          (^:once fn* [~conn-binding] ~@body))))
