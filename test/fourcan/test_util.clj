(ns fourcan.test-util
  (:require [fourcan.db.jdbc :as jdbc]
            [methodical.core :as m]
            [pretty.core :refer [PrettyPrintable]])
  (:import [java.sql Driver DriverManager]
           java.util.Properties
           javax.sql.DataSource))

(defn- proxy-data-source
  (^DataSource [jdbc-url]
   (proxy-data-source (DriverManager/getDriver jdbc-url) jdbc-url))

  (^DataSource [^Driver driver ^String jdbc-url]
   (reify
     DataSource
     (getConnection [_]
       (.connect driver jdbc-url nil))
     (getConnection [_ username password]
       (let [properties (Properties.)]
         (doseq [[k v] {"user" username, "password" password}]
           (when (some? k)
             (.setProperty properties k (name v))))
         (.connect driver jdbc-url properties)))

     PrettyPrintable
     (pretty [_]
       (list 'proxy-data-source (.getName (class driver)) jdbc-url)))))

(def ^:dynamic *test-data-source* nil)

(defn- ^DataSource test-data-source [close-delay]
  (or *test-data-source*
      (proxy-data-source (format "jdbc:h2:mem:test-%d;DB_CLOSE_DELAY=%d" (rand-int (Integer/MAX_VALUE)) close-delay))))

(defn do-with-test-data-source [thunk]
  (thunk)
  #_(binding [*test-data-source* (test-data-source 30)]
    (try
      (thunk)
      (finally
        (jdbc/execute! "SHUTDOWN")))))

(defmacro with-test-data-source [& body]
  `(do-with-test-data-source (fn [] ~@body)))

;; NOCOMMIT
(defn create-birds-table! []
  (jdbc/execute! "CREATE TABLE \"bird\" (\"id\" INTEGER, \"species\" TEXT);")
  (jdbc/execute! "INSERT INTO \"bird\" (\"id\", \"species\") VALUES (1, 'Toucan'), (2, 'Pigeon'), (3, 'Pelican');"))

(defonce ^:private data-source
  (delay (test-data-source -1)))

(m/defmethod jdbc/data-source :default
  [_]
  @data-source #_(test-data-source 30))

(m/defmethod jdbc/after-read [nil org.h2.jdbc.JdbcClob]
  [_ ^org.h2.jdbc.JdbcClob clob]
  (.getSubString clob 1 (.length clob)))

;; TODO - after-read should support dispatching with multiple defaults!
(m/defmethod jdbc/after-read [:bird org.h2.jdbc.JdbcClob]
  [_ ^org.h2.jdbc.JdbcClob clob]
  (.getSubString clob 1 (.length clob)))
