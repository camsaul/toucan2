(ns bluejdbc.connection
  (:require [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [clojure.string :as str]
            [methodical.core :as m])
  (:import [java.sql Connection Driver DriverManager ResultSet Statement]
           javax.sql.DataSource))

(def ^:dynamic *include-connection-info-in-exceptions* false) ; EXPERIMENTAL

(defmacro ^:private try-catch-when-include-connection-info-in-exceptions-enabled
  {:style/indent 0}
  [& args]
  (let [body       (butlast args)
        catch-form (last args)]
    `(let [thunk# (fn [] ~@body)]
       (if *include-connection-info-in-exceptions*
         (try
           (thunk#)
           ~catch-form)
         (thunk#)))))

(m/defmulti driver
  "Coerce something to a `java.sql.Driver`."
  {:arglists '(^java.sql.Driver [driverable])}
  u/dispatch-on-first-arg)

(m/defmethod driver java.sql.Driver
  [this]
  this)

(m/defmethod driver Class
  [^Class klass]
  (driver (.newInstance klass)))

(m/defmethod driver String
  [s]
  (if (str/starts-with? s "jdbc:")
    (java.sql.DriverManager/getDriver s)
    (driver (Class/forName s))))

(m/defmulti connection*
  {:arglists '([connectable options])}
  u/dispatch-on-first-arg)

(m/defmethod connection* :around :default
  [connectable options]
  (let [conn (next-method connectable options)]
    (assert (and (sequential? conn)
                 (#{:new :existing} (first conn))
                 (instance? java.sql.Connection (second conn)))
            (str "connection* should return a pair like [new-or-existing connection]. Got: " (pr-str conn)
                 " Input: " (pr-str connectable)))
    conn))

(defn connection
  "Return a tuple like `[new-or-existing Connection]`."
  ([connectable]         (connection* connectable nil))
  ([connectable options] (connection* connectable options)))

(m/defmethod connection* Connection
  [conn _]
  [:existing conn])

(m/defmethod connection* clojure.lang.Fn
  [f options]
  (connection* (f options) options))

(defn create-connection-from-url!
  "Create a new JDBC connection from a JDBC URL."
  ^Connection [^String s {driverable       :connection/driver
                          ^String user     :connection/user
                          ^String password :connection/password
                          properties       :connection/properties
                          :as              options}]
  (log/trace "Creating new Connection from JDBC connection string")
  (cond
    driverable         (.connect (driver driverable) s (u/->Properties properties))
    (or user password) (DriverManager/getConnection s user password)
    properties         (DriverManager/getConnection s (u/->Properties properties))
    :else              (DriverManager/getConnection s)))

(m/defmethod connection* String
  [s options]
  [:new (create-connection-from-url! s options)])

(defn- connection-with-class-name ^Connection [class-name ^String jdbc-url ^java.util.Properties properties]
  (let [^Class klass (cond
                       (string? class-name) (Class/forName class-name)
                       (symbol? class-name) (Class/forName (name class-name))
                       :else                class-name)]
    (assert (instance? Class klass))
    (assert (isa? klass java.sql.Driver) (format "%s is not a subclass of java.sql.Driver" (.getCanonicalName klass)))
    (let [^Driver driver (.newInstance klass)]
      (.connect driver jdbc-url properties))))

;; TODO -- support both clojure.java.jdbc-style maps and jdbc.next-style maps
(defn create-connection-from-clojure-java-jdbc-map!
  "Create a new JDBC connection from a `clojure.java.jdbc`-style map."
  ^Connection [m options]
  (log/trace "Getting new connection with legacy `clojure.java.jdbc` details map")
  (cond
    ;; TODO -- this should probably throw an Exception because it's going to screw up lifecycles
    (:connection m)
    (connection* (:connection m) options)

    (:datasource m)
    (connection* (:datasource m) options)

    ((every-pred :subprotocol :subname) m)
    (let [{:keys [classname subprotocol subname]} m
          jdbc-url                                (format "jdbc:%s:%s" (name subprotocol) subname)
          properties                              (u/->Properties (dissoc m :classname :subprotocol :subname))]
      (if classname
        (connection-with-class-name classname jdbc-url properties)
        (DriverManager/getConnection jdbc-url properties)))

    (not (:subprotocol m))
    (throw (ex-info "Can't create Connection from clojure.java.jdbc-style map: missing :subprotocol"
                    {:keys (vec (keys m))}))

    (not (:subname m))
    (throw (ex-info "Can't create Connection from clojure.java.jdbc-style map: missing :subname"
                    {:keys (vec (keys m))}))))

(m/defmethod connection* clojure.lang.IPersistentMap
  [m options]
  [:new (create-connection-from-clojure-java-jdbc-map! m options)])

(defn create-connection-from-datasource!
  "Create a new JDBC connection from a DataSource."
  ^Connection [^DataSource data-source
               {^String user     :connection/user
                ^String password :connection/password
                :as              options}]
  (log/trace "Getting new Connection from DataSource")
  (if (or user password)
    (.getConnection data-source user password)
    (.getConnection data-source)))

(m/defmethod connection* DataSource
  [datasource options]
  [:new (create-connection-from-datasource! datasource options)])

(defn do-with-connection
  "Impl for `with-connection`."
  [connectable options f]
  (let [[new-or-existing ^Connection conn] (try-catch-when-include-connection-info-in-exceptions-enabled
                                             (connection connectable options)
                                             (catch Throwable e
                                               (throw (ex-info "Error getting Connection"
                                                               {:connectable connectable}
                                                               e))))]
    (try-catch-when-include-connection-info-in-exceptions-enabled
      (case new-or-existing
        :new      (with-open [conn conn]
                    (f conn))
        :existing (f conn))
      (catch Throwable e
        (throw (ex-info "Error executing body of with-connection"
                        {:connectable connectable, :connection conn, :options options}
                        e))))))

(defmacro with-connection
  "Execute `body` with `conn-binding` bound to a `Connection`. If `connectable` is already a `Connection`, `body` is
  executed using that `Connection`; if `connectable` is something else like a JDBC URL, a new `Connection` will be
  created for the duration of `body` and closed afterward.

  You can use this macro to accept either a `Connection` or something that can be used to create a `Connection` and
  handle either case appropriately."
  {:arglists '([[conn-binding connectable] & body] [[conn-binding connectable options] & body])}
  [[conn-binding connectable options] & body]
  (u/assert-no-recurs "with-connection" body)
  `(do-with-connection ~connectable ~options (fn [~(vary-meta conn-binding assoc :tag 'java.sql.Connection)]
                                               ~@body)))

(m/defmulti db-type
  {:arglists '([x])}
  u/dispatch-on-first-arg)

(m/defmethod db-type Connection
  [^Connection conn]
  (keyword (str/lower-case (.. conn getMetaData getDatabaseProductName))))

(m/defmethod db-type Statement
  [^Statement statement]
  (db-type (.getConnection statement)))

(m/defmethod db-type ResultSet
  [^ResultSet result-set]
  (db-type (.getStatement result-set)))

(def ^:private ^{:arglists '([db-type])} load-integrations-if-needed!
  (memoize
   (fn [db-type]
     (try
       (locking clojure.lang.RT/REQUIRE_LOCK
         (let [ns-symb (symbol (str (format "bluejdbc.integrations.%s" (name db-type))))]
           (log/trace (pr-str (list 'require ns-symb)))
           (require ns-symb)))
       (catch Throwable _)))))

(m/defmethod db-type :after :default
  [db-type]
  (load-integrations-if-needed! db-type)
  db-type)
