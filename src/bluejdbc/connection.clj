(ns bluejdbc.connection
  (:require [bluejdbc.driver :as driver]
            [bluejdbc.options :as options]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [methodical.core :as m]
            [potemkin.types :as p.types])
  (:import [java.sql Connection DriverManager]
           javax.sql.DataSource))

(u/define-enums transaction-isolation-level Connection #"^TRANSACTION_")

(p.types/defprotocol+ NewConnection
  "Protocol for anything that can used to create a new `Connection`."
  (create-connection ^java.sql.Connection [this options]
    "Coerce `this` to a `Connection`."))

(defn url->Connection
  [^String s {driver           :connection/driver
              ^String user     :connection/user
              ^String password :connection/password
              properties       :connection/properties
              :as              options}]
  (log/trace "Creating new Connection from JDBC connection string")
  (let [conn (cond
               driver
               (.connect (driver/driver driver) s (options/->Properties properties))

               (or user password)
               (DriverManager/getConnection s user password)

               properties
               (DriverManager/getConnection s (options/->Properties properties))

               :else
               (DriverManager/getConnection s))]
    (options/with-options conn (dissoc options :connection/driver :connection/user :connection/password :connection/properties))))

(defn map->Connection
  ^Connection [m options]
  (log/trace "Getting new connection with legacy `clojure.java.jdbc` details map")
  (cond
    (:connection m)
    (create-connection (:connection m) options)

    (:datasource m)
    (create-connection (:datasource m) options)

    :else
    ;; TODO -- other stuff
    (throw (ex-info "TODO" {}))
    #_(options/with-options m options)))

(defn DataSource->Connection
  ^Connection [^javax.sql.DataSource data-source {^String user :connection/user, ^String password :connection/password, :as options}]
  (log/trace "Getting new Connection from DataSource")
  (let [conn (if (or user password)
               (.getConnection data-source user password)
               (.getConnection data-source))]
    (options/with-options conn (dissoc options :connection/user :connection/password))))

(extend-protocol NewConnection
  ;; this

  ;; JDBC connection URL
  String
  (create-connection [s options]
    (url->Connection s options))

  javax.sql.DataSource
  (create-connection [data-source options]
    (DataSource->Connection data-source options))

  ;; legacy `clojure.java.jdbc`-style map
  clojure.lang.IPersistentMap
  (create-connection [m options]
    (map->Connection m options)))

(m/defmethod options/with-option [Connection :connection/auto-commit?]
  [^Connection conn _ auto-commit?]
  (log/tracef "Set Connection auto commit -> %s" (boolean auto-commit?))
  (.setReadOnly conn (boolean auto-commit?)))

(m/defmethod options/with-option [Connection :connection/catalog]
  [^Connection conn _ catalog]
  (log/tracef "Set Connection catalog -> %s" catalog)
  (doto conn
    (.setCatalog (str catalog))))

(m/defmethod options/with-option [Connection :connection/client-info]
  [^Connection conn _ properties]
  (log/tracef "Set Connection client info -> %s" properties)
  (doto conn
    (.setClientInfo conn (options/->Properties properties))))

(m/defmethod options/with-option [Connection :result-set/holdability]
  [^Connection conn _ holdability]
  (log/tracef "Set Connection holdability -> %s" (u/reverse-lookup result-set/holdability holdability))
  (doto conn
    (.setHoldability (result-set/holdability holdability))))

(m/defmethod options/with-option [Connection :connection/read-only?]
  [^Connection conn _ read-only?]
  (log/tracef "Set Connection read only -> %s" (boolean read-only?))
  (doto conn
    (.setReadOnly (boolean read-only?))))

(m/defmethod options/with-option [Connection :connection/schema]
  [^Connection conn _ schema]
  (log/tracef "Set Connection schema -> %s" schema)
  (doto conn
    (.setSchema (str schema))))

(m/defmethod options/with-option [Connection :connection/transaction-isolation]
  [^Connection conn _ level]
  (log/tracef "Set Connection transaction isolation -> %s" (u/reverse-lookup transaction-isolation-level level))
  (doto conn
    (.setTransactionIsolation (transaction-isolation-level level))))

(defn connection
  (^Connection [source]
   (create-connection source nil))

  (^Connection [source options]
   (create-connection source options)))

(defn do-with-connection [source options f]
  (if (instance? java.sql.Connection source)
    (f (options/with-options source options))
    (with-open [conn (connection source options)]
      (f conn))))

(defmacro with-connection
  [[conn-binding source options] & body]
  `(do-with-connection ~source ~options (fn [~(vary-meta conn-binding assoc :tag 'java.sql.Connection)] ~@body)))
