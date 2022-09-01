(ns toucan2.connection
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [next.jdbc :as next.jdbc]
   [next.jdbc.transaction :as next.jdbc.transaction]
   [pretty.core :as pretty]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(def ^:dynamic *current-connectable*
  "The current connectable or connection. If you get a connection with [[with-connection]] or [[with-transaction]], it
  will be bound here. You can also bind this yourself to a connectable or connection, and Toucan methods called without
  an explicit will connectable will use it rather than the `:default` connection."
  nil)

;;; TODO -- Should this have an additional `options` parameter like [[with-connection]] does? Or is the current strategy
;;; of using a dynamic var working ok? If this has an options parameter then that would tend to bubble up and pretty
;;; soon everything has an options parameter which ruins our life (speaking from pre-rewrite version of Toucan 2 where
;;; originally everything did have an options parameter)

(m/defmulti do-with-connection
  {:arglists '([connectable₁ f])}
  u/dispatch-on-first-arg
  :default-value ::default)

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
      bound-fn*
      (next-method connectable (^:once fn* [conn]
                                (binding [*current-connectable* conn]
                                  (f conn)))))))

(defmacro with-connection
  {:arglists '([[connection-binding connectable] & body]
               [[connection-binding connectable] & body])}
  [[connection-binding connectable] & body]
  `(do-with-connection
    ~connectable
    (^:once fn* [~(or connection-binding '_)] ~@body)))

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
  (do-with-connection (next.jdbc/get-datasource m) f))

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
  "`options` are options for determining what type of transaction we'll get. See dox for [[with-transaction]] for more
  information."
  {:arglists '([connection₁ options f])}
  u/dispatch-on-first-arg
  :default-value ::default)

(m/defmethod do-with-transaction :around ::default
  [connection options f]
  (log/debugf :execute "do with transaction %s %s" options (some-> connection class .getCanonicalName symbol))
  (let [f* (^:once fn* [conn]
            (binding [*current-connectable* conn]
              (f conn)))]
    (next-method connection options f*)))

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

  `:nested-transaction-rule` must be one of `#{:allow :ignore :prohibit}`, a set of possibilities shamelessly borrowed
  from `next.jdbc`. For non-JDBC implementations, you should treat `:allow` as the default behavior if unspecified."
  {:style/indent 1, :arglists '([[conn-binding connectable options?] & body])}
  [[conn-binding connectable options] & body]
  `(with-connection [conn# ~connectable]
     (do-with-transaction conn# ~options (^:once fn* [~(or conn-binding '_)] ~@body))))

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
