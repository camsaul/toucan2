(ns toucan2.connection
  "#### Connection Resolution

  The rules for determining which connection to use are as follows. These are tried in order until one returns
  non-nil:

  1. The connectable specified in the function arguments.

  2. The [[toucan2.connection/*current-connectable*]], if bound. This is bound automatically when
     using [[with-connection]] or [[with-transaction]]

  3. The [[toucan2.model/default-connectable]] for the model resolved from the `modelable` in the function arguments;

  4. The `:default` implementation of [[toucan2.connection/do-with-connection]]

  You can define a 'named' connectable such as `::db` by adding an implementation
  of [[toucan2.connection/do-with-connection]], or use things like JDBC URL connection strings or [[clojure.java.jdbc]]
  connection properties maps directly.

  IMPORTANT CAVEAT! Positional connectables will be used in preference to [[*current-connectable*]], even when it was
  bound by [[with-transaction]] -- this means your query will run OUTSIDE of the current transaction! Sometimes, this is
  what you want, because maybe a certain query is meant to run against a different database! Usually, however, it is
  not! So in that case you can either do something like

  ```clj
  (t2/query (or conn/*current-connectable* ::my-db) ...)
  ```

  to use the current connection if it exists, or define your named connectable method like

  ```clj
  (m/defmethod conn/do-with-connection ::my-db
    [_connectable f]
    (conn/do-with-connection
     (if (and conn/*current-connectable*
              (not= conn/*current-connectable* ::my-db))
         conn/*current-connectable*
         \"jdbc:postgresql://...\")
     f))
  ```

  This, however, is super annoying! So I might reconsider this behavior in the future.

  For reducible queries, the connection is not resolved until the query is executed, so you may create a reducible query
  with no default connection available and execute it later with one bound. (This also means
  that [[toucan2.execute/reducible-query]] does not capture dynamic bindings such
  as [[toucan2.connection/*current-connectable*]] -- you probably wouldn't want it to, anyway, since we have no
  guarantees and open connection will be around when we go to use the reducible query later.)"
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [next.jdbc :as next.jdbc]
   [next.jdbc.transaction :as next.jdbc.transaction]
   [pretty.core :as pretty]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment types/keep-me)

(def ^:dynamic *current-connectable*
  "The current connectable or connection. If you get a connection with [[with-connection]] or [[with-transaction]], it
  will be bound here. You can also bind this yourself to a connectable or connection, and Toucan methods called without
  an explicit will connectable will use it rather than the `:default` connection."
  nil)

(m/defmulti do-with-connection
  "Take a *connectable*, get a connection of some sort from it, and execute `(f connection)` with an open connection. A
  normal implementation might look something like:

  ```clj
  (m/defmethod t2.conn/do-with-connection ::my-connectable
    [_connectable f]
    (with-open [conn (get-connection)]
      (f conn)))
  ```

  Another common use case is to define a 'named' connectable that acts as an alias for another more complicated
  connectable, such as a JDBC connection string URL. You can do that like this:

  ```clj
  (m/defmethod t2.conn/do-with-connection ::a-connectable
    [_connectable f]
    (t2.conn/do-with-connection
     \"jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam\"
     f))
  ```"
  {:arglists            '([connectable₁ f])
   :defmethod-arities   #{2}
   :dispatch-value-spec ::types/dispatch-value.keyword-or-class}
  u/dispatch-on-first-arg
  :default-value ::default)

(defn- bind-current-connectable-fn
  "Wrap functions as passed to [[do-with-connection]] or [[do-with-transaction]] in a way that
  binds [[*current-connectable*]]."
  [f]
  {:pre [(fn? f)]}
  (^:once fn* [conn]
   (binding [*current-connectable* conn]
     (f conn))))

(m/defmethod do-with-connection :around ::default
  [connectable f]
  (assert (fn? f))
  ;; add the connection class or pretty representation rather than the connection type itself to avoid leaking sensitive
  ;; creds
  (let [connectable-class (if (instance? pretty.core.PrettyPrintable connectable)
                            (pretty/pretty connectable)
                            (protocols/dispatch-value connectable))]
    (log/debugf :execute "Resolve connection %s" connectable-class)
    (u/try-with-error-context ["resolve connection" {::connectable connectable-class}]
      (next-method connectable (bind-current-connectable-fn f)))))

(defmacro with-connection
  "Execute `body` with an open connection. There are three ways to use this.

  With no args in the bindings vector, `with-connection` will use the *current connection* -- [[*current-connectable*]]
  if one is bound, or the *default connectable* if not. See docstring for [[toucan2.connection]] for more information.

  ```clj
  (t2/with-connection []
    ...)
  ```

  With one arg, `with-connection` still uses the *current connection*, but binds it to something (`conn` in the example
  below):

  ```clj
  (t2/with-connection [conn]
    ...)
  ```

  If you're using the default JDBC backend, `conn` will be an instance of `java.sql.Connection`. Since Toucan 2 is also
  written to work with other backend besides JDBC, `conn` does *not* include `java.sql.Connection` `:tag` metadata! If
  you're doing Java interop with `conn`, make sure to tag it yourself:

  ```clj
   (t2/with-connection [^java.sql.Connection conn]
     (let [metadata (.getMetaData conn)]
       ...))
  ```

  With a connection binding *and* a connectable:

  ```clj
  (t2/with-connection [conn ::my-connectable]
    ...)
  ```

  This example gets a connection by calling [[do-with-connection]] with `::my-connectable`, ignoring the *current
  connection*."
  {:arglists '([[connection-binding]             & body]
               [[connection-binding connectable] & body])}
  [[connection-binding connectable] & body]
  `(do-with-connection
    ~connectable
    (^:once fn* with-connection* [~(or connection-binding '_)] ~@body)))

(s/fdef with-connection
  :args (s/cat :bindings (s/spec (s/cat :connection-binding (s/? symbol?)
                                        :connectable        (s/? any?)))
               :body (s/+ any?))
  :ret  any?)

;;; method if this is called with something we don't know how to handle or if no default connection is defined. This is
;;; separate from `:default` so if you implement `:default` you don't accidentally have that get called for unknown
;;; connectables
(m/defmethod do-with-connection ::default
  [connectable _f]
  (throw (ex-info (format "Don't know how to get a connection from ^%s %s. Do you need to implement %s for %s?"
                          (some-> connectable class .getCanonicalName)
                          (pr-str connectable)
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

(m/defmethod do-with-connection nil
  "`nil` means use the current connection.

  The difference between `nil` and using [[*current-connectable*]] directly is that this waits until it gets resolved
  by [[do-with-connection]] to get the value for [[*current-connectable*]]. For a reducible query this means you'll get
  the value at the time you reduce the query rather than at the time you build the reducible query."
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
  "Implementation for map connectables. Treats them as a `clojure.java.jdbc`-style connection spec map, converting them to
  a `java.sql.DataSource` with [[next.jdbc/get-datasource]]."
  [m f]
  (do-with-connection (next.jdbc/get-datasource m) f))

;;; for record types that implement `DataSource`, prefer the `DataSource` impl over the map impl.
(m/prefer-method! #'do-with-connection javax.sql.DataSource clojure.lang.IPersistentMap)

;;;; connection string support

(defn connection-string-protocol
  "Extract the protocol part of a `connection-string`.

  ```clj
  (connection-string-protocol \"jdbc:postgresql:...\")
  =>
  \"jdbc\"
  ```"
  [connection-string]
  (when (string? connection-string)
    (second (re-find #"^(?:([^:]+):)" connection-string))))

(m/defmulti do-with-connection-string
  "Implementation of [[do-with-connection]] for strings. Dispatches on the [[connection-string-protocol]] of the string,
  e.g. `\"jdbc\"` for `\"jdbc:postgresql://localhost:3000/toucan\"`."
  {:arglists '([^java.lang.String connection-string f])}
  (fn [connection-string _f]
    (connection-string-protocol connection-string)))

(m/defmethod do-with-connection String
  "Implementation for Strings. Hands off to [[do-with-connection-string]]."
  [connection-string f]
  (do-with-connection-string connection-string f))

(m/defmethod do-with-connection-string "jdbc"
  "Implementation of `do-with-connection-string` (and thus [[do-with-connection]]) for all strings starting with `jdbc:`.
  Calls `java.sql.DriverManager/getConnection` on the connection string."
  [^String connection-string f]
  (with-open [conn (java.sql.DriverManager/getConnection connection-string)]
    (f conn)))

(m/defmulti do-with-transaction
  "`options` are options for determining what type of transaction we'll get. See dox for [[with-transaction]] for more
  information."
  {:arglists '([connection₁ options f])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod do-with-transaction :around ::default
  [connection options f]
  (log/debugf :execute "do with transaction %s %s" options (some-> connection class .getCanonicalName symbol))
  (next-method connection options (bind-current-connectable-fn f)))

(m/defmethod do-with-transaction java.sql.Connection
  [^java.sql.Connection conn options f]
  (let [nested-tx-rule (get options :nested-transaction-rule :allow)
        options        (dissoc options :nested-transaction-rule)]
    (log/debugf :execute "do with JDBC transaction (nested rule: %s) with options %s" nested-tx-rule options)
    (binding [next.jdbc.transaction/*nested-tx* nested-tx-rule]
      (next.jdbc/with-transaction [t-conn conn options]
        (f t-conn)))))

(defmacro with-transaction
  "Gets a connection with [[with-connection]], and executes `body` within that transaction.

  An `options` map, if specified, determine what sort of transaction we're asking for (stuff like the read isolation
  level and what not). One key, `:nested-transaction-rule`, is handled directly in Toucan 2; other options are passed
  directly to the underlying implementation, such as [[next.jdbc.transaction]].

  `:nested-transaction-rule` must be one of `#{:allow :ignore :prohibit}`, a set of possibilities borrowed from
  [[next.jdbc]]. For non-JDBC implementations, you should treat `:allow` as the default behavior if unspecified."
  {:style/indent 1, :arglists '([[conn-binding connectable options?] & body])}
  [[conn-binding connectable options] & body]
  `(with-connection [conn# ~connectable]
     (do-with-transaction conn# ~options (^:once fn* with-transaction* [~(or conn-binding '_)] ~@body))))

(s/def :toucan2.with-transaction-options/nested-transaction-rule
  (s/nilable #{:allow :ignore :prohibit}))

(s/def ::with-transaction-options
  (s/keys :opt-un [:toucan2.with-transaction-options/nested-transaction-rule]))

(s/fdef with-transaction
  :args (s/cat :bindings (s/spec (s/cat :connection-binding (s/? symbol?)
                                        :connectable        (s/? any?)
                                        :options            (s/? ::with-transaction-options)))
               :body (s/+ any?))
  :ret  any?)
