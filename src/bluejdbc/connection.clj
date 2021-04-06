(ns bluejdbc.connection
  (:require [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc])
  (:import java.sql.Connection))

(def ^:dynamic *include-connection-info-in-exceptions*
  "Whether exceptions that occur when acquiring a Connection should include the connection details and options. By
  default, this is false, to avoid leaking sensitive information like passwords. You can this for debugging purposes."
  false)

(def ^:dynamic ^java.sql.Connection *connection*
  "Connection that was bound by `with-connection`, used by default when connection is unspecified."
  nil)

(def ^:dynamic ^java.sql.Connection *options*
  "Options associated with the Connection bound by `with-connection` or `transaction`."
  nil)

(def ^:dynamic ^java.sql.Connection *transaction-connection*
  "Connection that was bound by `transaction`, used by default inside a transaction when connection is unspecified."
  nil)

(m/defmulti default-options
  {:arglists '([connectable])}
  u/dispatch-on-first-arg)

(m/defmethod default-options :default
  [_]
  nil)

(m/defmethod default-options :current
  [_]
  (or *options*
      (default-options :default)))

(m/defmulti connection*
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg)

(m/defmethod connection* :around :default
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

(m/defmethod connection* :default
  [connectable options]
  [:new (next.jdbc/get-connection connectable (:connection options))])

(m/defmethod connection* :current
  [connectable options]
  (or (when *transaction-connection*
        [:existing *transaction-connection*])
      (when *connection*
        [:existing *connection*])
      (connection* :default options)))

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
    (binding [*connection* conn
              *options*    options]
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
  (let [connectable (or connectable :current)
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
