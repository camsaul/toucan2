(ns bluejdbc.connection
  (:require [bluejdbc.driver :as driver]
            [bluejdbc.options :as options]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [methodical.core :as m]
            [potemkin.types :as p.types]
            [pretty.core :as pretty])
  (:import [java.sql Connection DriverManager]
           javax.sql.DataSource))

(u/define-enums transaction-isolation-level Connection #"^TRANSACTION_")

(u/define-proxy-class ProxyConnection Connection [conn mta opts]
  pretty/PrettyPrintable
  (pretty [_]
          (list 'proxy-connection conn opts))

  options/Options
  (options [_]
           opts)

  (with-options* [_ new-options]
    (ProxyConnection. conn mta new-options))

  clojure.lang.IObj
  (meta [_]
        mta)

  (withMeta [_ new-meta]
            (ProxyConnection. conn new-meta opts))

  Connection
  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql]
   (stmt/proxy-prepared-statement (.prepareStatement conn sql) (assoc opts :_connection this)))

  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql ^"[Ljava.lang.String;" column-names]
   (-> (.prepareStatement conn sql column-names)
       (stmt/proxy-prepared-statement (assoc opts :_connection this))))

  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql ^ints column-indexes]
   (-> (.prepareStatement conn sql column-indexes)
       (stmt/proxy-prepared-statement (assoc opts :_connection this))))

  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql ^int auto-generated-keys]
   (-> (.prepareStatement conn sql auto-generated-keys)
       (stmt/proxy-prepared-statement (assoc opts :_connection this))))

  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql ^int result-set-type ^int result-set-concurrency]
   (-> (.prepareStatement conn sql result-set-type result-set-concurrency)
       (stmt/proxy-prepared-statement (assoc opts
                                             :_connection this
                                             :result-set/type (u/reverse-lookup rs/type result-set-type)
                                             :result-set/concurrency (u/reverse-lookup rs/concurrency result-set-concurrency)))))

  (^java.sql.PreparedStatement prepareStatement
   [this ^String sql ^int result-set-type ^int result-set-concurrency ^int result-set-holdability]
   (-> (.prepareStatement conn sql result-set-type result-set-concurrency result-set-holdability)
       (stmt/proxy-prepared-statement (assoc opts
                                             :_connection this
                                             :result-set/type (u/reverse-lookup rs/type result-set-type)
                                             :result-set/concurrency (u/reverse-lookup rs/concurrency result-set-concurrency)
                                             :result-set/holdability (u/reverse-lookup rs/holdability result-set-holdability))))))

(defn proxy-connection
  "Wrap a `Connection` in a `ProxyConnection`, if not already wrapped."
  (^ProxyConnection [conn]
   (proxy-connection conn nil))

  (^ProxyConnection [conn options]
   (when conn
     (if (instance? ProxyConnection conn)
       (options/with-options conn options)
       (do
         (options/set-options! conn options)
         (ProxyConnection. conn nil options))))))

(p.types/defprotocol+ CreateConnection
  "Protocol for anything that can used to create a new `Connection`."
  (connect!* ^bluejdbc.connection.ProxyConnection [this options]
    "Create a new JDBC connection from `this`."))

(defn create-connection-from-url!
  "Create a new JDBC connection from a JDBC URL."
  ^ProxyConnection [^String s {driver           :connection/driver
                               ^String user     :connection/user
                               ^String password :connection/password
                               properties       :connection/properties
                               :as              options}]
  (log/trace "Creating new Connection from JDBC connection string")
  (let [conn    (cond
                  driver             (.connect (driver/driver driver) s (options/->Properties properties))
                  (or user password) (DriverManager/getConnection s user password)
                  properties         (DriverManager/getConnection s (options/->Properties properties))
                  :else              (DriverManager/getConnection s))
        options (assoc options :connection/type (keyword (second (re-find #"^jdbc:([^:]+):" s))))]
    (proxy-connection conn options)))

(defn create-connection-from-clojure-java-jdbc-map!
  "Create a new JDBC connection from a `clojure.java.jdbc`-style map."
  ^ProxyConnection [m options]
  (log/trace "Getting new connection with legacy `clojure.java.jdbc` details map")
  (cond
    ;; TODO -- this should probably throw an Exception because it's going to screw up lifecycles
    (:connection m)
    (connect!* (:connection m) options)

    (:datasource m)
    (connect!* (:datasource m) options)

    :else
    ;; TODO -- other stuff
    (throw (ex-info "TODO" {}))))

(defn create-connection-from-datasource!
  "Create a new JDBC connection from a DataSource."
  ^ProxyConnection [^javax.sql.DataSource data-source {^String user :connection/user, ^String password :connection/password, :as options}]
  (log/trace "Getting new Connection from DataSource")
  (let [conn (if (or user password)
               (.getConnection data-source user password)
               (.getConnection data-source))]
    (proxy-connection conn options)))

(extend-protocol CreateConnection
  ;; JDBC connection URL
  String
  (connect!* [s options]
    (create-connection-from-url! s options))

  javax.sql.DataSource
  (connect!* [data-source options]
    (create-connection-from-datasource! data-source options))

  ;; legacy `clojure.java.jdbc`-style map
  clojure.lang.IPersistentMap
  (connect!* [m options]
    (create-connection-from-clojure-java-jdbc-map! m options)))

(defn connect!
  "Create a new JDBC connection from `source`."
  (^ProxyConnection [connectable]
   (connect!* connectable nil))

  (^ProxyConnection [connectable options]
   (connect!* connectable options)))

(defn do-with-connection
  "Impl for `with-connection`."
  [connectable options f]
  (if (instance? Connection connectable)
    (let [conn (proxy-connection connectable options)]
      (f conn))
    (with-open [conn (connect! connectable options)]
      (f conn))))

(defmacro with-connection
  "Execute `body` with `conn-binding` bound to a `Connection`. If `connectable` is already a `Connection`, `body` is executed
  using that `Connection`; if `connectable` is something else like a JDBC URL, a new `Connection` will be created for the
  duration of `body` and closed afterward.

  You can use this macro to accept either a `Connection` or something that can be used to create a `Connection` and
  handle either case appropriately."
  {:arglists '([[conn-binding connectable] & body] [[conn-binding connectable options] & body])}
  [[conn-binding connectable options] & body]
  `(do-with-connection ~connectable ~options (fn [~(vary-meta conn-binding assoc :tag 'bluejdbc.connection.ProxyConnection)]
                                               ~@body)))


;;;; Option handling

(m/defmethod options/set-option! [Connection :connection/auto-commit?]
  [^Connection conn _ auto-commit?]
  (.setReadOnly conn (boolean auto-commit?)))

(m/defmethod options/set-option! [Connection :connection/catalog]
  [^Connection conn _ catalog]
  (doto conn
    (.setCatalog (str catalog))))

(m/defmethod options/set-option! [Connection :connection/client-info]
  [^Connection conn _ properties]
  (doto conn
    (.setClientInfo conn (options/->Properties properties))))

(m/defmethod options/set-option! [Connection :result-set/holdability]
  [^Connection conn _ holdability]
  (doto conn
    (.setHoldability (rs/holdability holdability))))

(m/defmethod options/set-option! [Connection :connection/read-only?]
  [^Connection conn _ read-only?]
  (doto conn
    (.setReadOnly (boolean read-only?))))

(m/defmethod options/set-option! [Connection :connection/schema]
  [^Connection conn _ schema]
  (doto conn
    (.setSchema (str schema))))

(m/defmethod options/set-option! [Connection :connection/transaction-isolation]
  [^Connection conn _ level]
  (doto conn
    (.setTransactionIsolation (transaction-isolation-level level))))
