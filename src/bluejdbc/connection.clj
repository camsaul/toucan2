(ns bluejdbc.connection
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc])
  (:import java.sql.Connection))

(def ^:dynamic *include-connection-info-in-exceptions*
  "Whether exceptions that occur when acquiring a Connection should include the connection details and options. By
  default, this is false, to avoid leaking sensitive information like passwords. You can this for debugging purposes."
  false)

;; TODO -- can we consolidate these dynamic variables (?)

#_(def ^:dynamic *current*
  :default)

(def ^:dynamic ^:deprecated  *connectable-for-new-connections*
  "Connectable to use to create new connections, overriding `:default`. Not bound by default; override this if you'd
  rather bind connectable dynamically instead of providing a `:default` implementation."
  nil)

(def ^:dynamic *current-connectable*
  "Connectable that was used to bound the current connection."
  nil)

(def ^:dynamic ^java.sql.Connection *connection*
  "Current Connection that was bound by `with-connection`, used by default when connection is unspecified."
  nil)

(def ^:dynamic ^java.sql.Connection *options*
  "Options associated with the Connection bound by `with-connection` or `transaction`."
  nil)

(def ^:dynamic ^java.sql.Connection *transaction-connection*
  "Connection that was bound by `transaction`, used by default inside a transaction when connection is unspecified."
  nil)

#_(defn- default-connectable-for-new-connections []
  (or *connectable-for-new-connections*
      :default))

(m/defmulti connectable
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod connectable ::default
  [connectable _]
  connectable)

(m/defmulti default-options
  {:arglists '([connectable])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod default-options ::default
  [_]
  nil)

(m/defmulti connection*
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod connection* :around ::default
  [connectable options]
  (let [conn (try
               (next-method connectable options)
               (catch Throwable e
                 (throw (ex-info (ex-message e)
                                 (if *include-connection-info-in-exceptions*
                                   {:connectable connectable
                                    :options     options}
                                   {})
                                 e))))]
    (assert (and (sequential? conn)
                 (#{:new :existing} (first conn))
                 (instance? java.sql.Connection (second conn)))
            (str "connection* should return a pair like [new-or-existing connection]. Got: " (pr-str conn)
                 " Input: " (pr-str connectable)))
    conn))

(m/defmethod connection* ::default
  [a-connectable options]
  (println "(connectable a-connectable options):" (connectable a-connectable options)) ; NOCOMMIT
  [:new (next.jdbc/get-connection (connectable a-connectable options) (:connection options))])

#_(m/defmethod connection* :current
  [a-connectable options]
  (or (when *transaction-connection*
        [:existing *transaction-connection*])
      (when *connection*
        [:existing *connection*])
      (next-method a-connectable options)))

(m/defmethod connection* Connection
  [conn _]
  [:existing conn])

(defn connection
  "Return a tuple like `[new-or-existing Connection]`."
  ([connectable]         (connection* connectable nil))
  ([connectable options] (connection* connectable (merge (default-options connectable) options))))

(defn do-with-connection
  "Impl for `with-connection`."
  [connectable options f]
  (let [options                            (merge (default-options connectable) options)
        [new-or-existing ^Connection conn] (connection connectable options)]
    (binding [*current-connectable* connectable
              *connection*          conn
              *options*             options]
      (case new-or-existing
        :existing (f conn)
        :new      (with-open [conn conn]
                    (f conn))))))

(defmacro with-connection
  "Execute `body` with `conn-binding` bound to a `Connection`. If `connectable` is already a `Connection`, `body` is
  executed using that `Connection`; if `connectable` is something else like a JDBC URL, a new `Connection` will be
  created for the duration of `body` and closed afterward.

  You can use this macro to accept either a `Connection` or something that can be used to create a `Connection` and
  handle either case appropriately."
  {:arglists '([[conn-binding connectable] & body] [[conn-binding connectable options] & body])}
  [[conn-binding connectable options] & body]
  (u/assert-no-recurs "with-connection" body)
  `(do-with-connection ~connectable ~options (^:once fn* [~(vary-meta conn-binding assoc :tag 'java.sql.Connection)]
                                              ~@body)))

(defn do-transaction [connectable options f]
  (let [connectable (or connectable conn/*transaction-connection* conn/*connection* conn/*current-connectable*)
        options     (merge (default-options connectable)
                           options)]
    (with-connection [conn connectable options]
      (next.jdbc/with-transaction [tx conn (:transaction options)]
        (binding [*transaction-connection* tx
                  *options*                options]
          (f tx))))))

(defmacro transaction
  "Execute `body` inside a JDBC transaction. Transaction is committed if body completes successfully; if body throws an
  Exception, transaction is aborted.

  `transaction` can be used with anything connectable, and binding the resulting connection is optional:

    (bluejdbc/with-connection [conn my-datasource]
      (transaction conn
        ...))"
  {:style/indent 1}
  [[conn-binding connectable options] & body]
  `(do-transaction
    ~connectable
    ~options
    (^:once fn* [~(vary-meta conn-binding assoc :tag 'java.sql.Connection)] ~@body)))
