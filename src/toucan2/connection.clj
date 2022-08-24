(ns toucan2.connection
  (:require
   [methodical.core :as m]
   [next.jdbc :as jdbc]
   [pretty.core :as pretty]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(def ^:dynamic *current-connectable*
  "The current connectable or connection. If you get a connection with [[with-connection]] or [[with-transaction]], it
  will be bound here. You can also bind this yourself to a connectable or connection, and Toucan methods called without
  an explicit will connectable will use it rather than the `:default` connection."
  nil)

(m/defmulti do-with-connection
  {:arglists '([connectable f])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod do-with-connection :around ::default
  [connectable f]
  (assert (fn? f))
  ;; add the connection class and connectable dispatch value rather than the connection type itself to avoid leaking
  ;; sensitive creds
  (u/try-with-error-context ["resolve connection" {::connectable (if (instance? pretty.core.PrettyPrintable connectable)
                                                                   (pretty/pretty connectable)
                                                                   (protocols/dispatch-value connectable))}]
    bound-fn*
    (next-method connectable (^:once fn* [conn]
                              (binding [*current-connectable* conn]
                                (f conn))))))

(defmacro with-connection
  {:arglists '([[connection-binding connectable] & body]
               [[connection-binding connectable] & body])}
  [[connection-binding connectable] & body]
  `(do-with-connection
    ~connectable
    (^:once fn* [~connection-binding] ~@body)))

;;; method if this is called with something we don't know how to handle or if no default connection is defined. This is
;;; separate from `:default` so if you implement `:default` you don't accidentally have that get called for unknown
;;; connectables
(m/defmethod do-with-connection ::default
  [connectable _f]
  (throw (ex-info (format "Don't know how to get a connection from ^%s %s. Do you need to implement %s for %s?"
                          (some-> connectable class .getCanonicalName)
                          (u/safe-pr-str connectable)
                          `do-with-connection
                          (protocols/dispatch-value connectable))
                  {:connectable connectable})))

;;; method called if there is no current connection.
(m/defmethod do-with-connection :default
  [_connectable _f]
  (throw (ex-info (format "No default Toucan connection defined. You can define one by implementing %s for :default. You can also implement %s for a model."
                          `do-with-connection
                          'toucan2.model/default-connectable)
                  {})))

;;; `nil` means use the current connection.
;;;
;;; The difference between `nil` and using [[*current-connectable*]] directly is that this waits until it gets resolved
;;; by [[do-with-connection]] to get the value for [[*current-connectable*]]. For a reducible query this means you'll
;;; get the value at the time you reduce the query rather than at the time you build the reducible query.
(m/defmethod do-with-connection nil
  [_connectable f]
  (let [current-connectable (if (nil? *current-connectable*)
                              :default
                              *current-connectable*)]
    (do-with-connection current-connectable f)))

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

(defn connection-string-protocol
  "Extract the protocol part of a `connection-string`.

    (connection-string-protocol \"jdbc:postgresql:...\")
    =>
    \"jdbc\""
  [connection-string]
  (when (string? connection-string)
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
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod do-with-transaction :around ::default
  [connection f]
  (u/with-debug-result [(list `do-with-transaction (some-> connection class .getCanonicalName symbol))]
    (next-method connection (^:once fn* [conn]
                             (binding [*current-connectable* conn]
                               (f conn))))))

(m/defmethod do-with-transaction java.sql.Connection
  [^java.sql.Connection conn f]
  (jdbc/with-transaction [t-conn conn]
    (f t-conn)))

(defmacro with-transaction
  {:style/indent 1}
  [[conn-binding connectable] & body]
  `(with-connection [conn# ~connectable]
     (do-with-transaction conn# (^:once fn* [~conn-binding] ~@body))))

;;; wraps a `reducible` and makes sure it is reduced inside a transaction.
(deftype ^:no-doc ReduceInTransaction [connectable reducible]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (with-transaction [_conn connectable]
      (reduce rf init reducible)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReduceInTransaction connectable reducible)))
