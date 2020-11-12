(ns bluejdbc.connection-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.options :as options]
            [bluejdbc.test :as test]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [potemkin.types :as p.types])
  (:import java.sql.DriverManager))

(deftest proxy-statement-test
  (testing "Make sure ProxyConnection.prepareStatement returns a ProxyPreparedStatement"
    (let [options {:x :y}]
      (with-open [conn (jdbc/connect! (test/jdbc-url) options)
                  stmt (.prepareStatement conn "SELECT 1;")]
        ;; in case classes get redefined
        (is (= "bluejdbc.connection.ProxyConnection"
               (some-> conn class .getCanonicalName)))
        (is (= "bluejdbc.statement.ProxyPreparedStatement"
               (some-> stmt class .getCanonicalName)))

        (testing "options should be passed along"
          (is (= {:x :y}
                 (select-keys (options/options stmt) [:x]))))

        (testing "the ProxyPreparedStatment should be able to return its parent ProxyConnection"
          (is (identical? conn (.getConnection stmt))))))))

(deftest proxy-database-metadata-test
  (testing "ProxyConnection.getMetaData should return a ProxyDatabaseMetaData"
    (with-open [conn (jdbc/connect! (test/jdbc-url))]
      (is (= "bluejdbc.metadata.ProxyDatabaseMetaData"
             (some-> (.getMetaData conn) class .getCanonicalName))))))

(p.types/defrecord+ ^:private MockConnection []
  java.sql.Connection

  java.lang.AutoCloseable
  (close [_])

  java.sql.Wrapper
  (unwrap [this interface]
    (when (instance? interface this)
      this)))

(p.types/defrecord+ ^:private TestDriver []
  java.sql.Driver
  (acceptsURL [_ url]
    (str/starts-with? url "jdbc:bluejdbc-test-driver:"))

  (connect [_ url properties]
    (assoc (MockConnection.)
           :url        url
           :properties (into {} (for [[k v] properties]
                                  [(keyword k) v])))))

(p.types/defrecord+ ^:private TestDriver2 []
  java.sql.Driver
  (acceptsURL [_ url]
    (str/starts-with? url "jdbc:bluejdbc-test-driver:"))

  (connect [_ url properties]
    (assoc (MockConnection.)
           :url        url
           :properties (into {} (for [[k v] properties]
                                  [(keyword k) v]))
           :driver TestDriver2)))

(defn- do-with-registered-driver
  ([thunk]
   (do-with-registered-driver (TestDriver.) thunk))

  ([driver thunk]
   (try
     (DriverManager/registerDriver driver)
     (thunk)
     (finally
       (DriverManager/deregisterDriver driver)))))

(defmacro ^:private with-test-driver [& body]
  `(do-with-registered-driver (fn [] ~@body)))

(deftest sanity-check
  (testing "Make sure our mock driver tooling works as expected"
    (let [mock-url "jdbc:bluejdbc-test-driver://localhost:1337/my_db?user=cam&password=cam"]
      (is (thrown? java.sql.SQLException (DriverManager/getConnection mock-url)))

      (with-test-driver
        (let [driver (DriverManager/getDriver mock-url)]
          (is (= "bluejdbc.connection_test.TestDriver"
                 (some-> driver class .getCanonicalName)))

          (is (= true
                 (.acceptsURL driver mock-url))))

        (with-open [conn (DriverManager/getConnection mock-url)]
          (is (= "bluejdbc.connection_test.MockConnection"
                 (some-> conn class .getCanonicalName))))))))

(deftest connection-from-url-test
  (testing "Should be able to get a Connection from a JDBC connection string"
    (with-test-driver
      (doseq [{:keys [description url expected options]}
              [{:description ""
                :url         "jdbc:bluejdbc-test-driver://localhost:1337/my_db?user=cam&password=cam"
                :options     {:x :y}
                :expected    {:properties {}}}
               {:description "with user and password keys"
                :url         "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                :options     {:connection/user "cam", :connection/password "cam"}
                :expected    {:properties {:user "cam", :password "cam"}}}
               {:description "with Properties"
                :url         "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                :options     {:connection/properties (options/->Properties {:user "cam", :password "cam"})}
                :expected    {:properties {:user "cam", :password "cam"}}}
               {:description "with :properties map; should automatically convert to Properties"
                :url         "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                :options     {:connection/properties {:user "cam", :password "cam"}}
                :expected    {:properties {:user "cam", :password "cam"}}}
               {:description "with a specific driver"
                :url         "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                :options     {:connection/driver (TestDriver2.)}
                :expected    {:properties {}
                              :driver     TestDriver2}}]]
        (testing description
          (with-open [conn (jdbc/connect! url options)]
            (is (= "bluejdbc.connection.ProxyConnection"
                   (some-> conn class .getCanonicalName)))
            (testing "Options should be set; :connection/type should be added"
              (is (= (assoc options :connection/type :bluejdbc-test-driver)
                     (options/options conn))))

            (let [unwrapped (.unwrap conn MockConnection)]
              (testing "Should be able to unwrap"
                (is (= "bluejdbc.connection_test.MockConnection"
                       (some-> unwrapped class .getCanonicalName)))

                (is (= (merge {:url url}
                              expected)
                       (into {} unwrapped)))))))))))

(deftest connection-from-map-test
  (testing "Should be able to get a Connection from a clojure.java.jdbc-style map"
    (let [m {:classname   "org.h2.Driver"
             :subprotocol "h2"
             :subname     "mem:bluejdbc_test;DB_CLOSE_DELAY=-1"}]
      (doseq [[description m] {"with classname"           m
                               "without classname"        (dissoc m :classname)
                               "subprotocol is a keyword" (update m :subprotocol keyword)
                               "classname is a Class"     (assoc m :classname org.h2.Driver)
                               "classname is a symbol"    (update m :classname symbol)}]
        (testing description
          (testing (format "\nm = %s" (pr-str m))
            (doseq [options [nil
                             {:option? true}]]
              (testing (format "\noptions = %s" (pr-str options))
                (with-open [conn (jdbc/connect! m options)]
                  (is (instance? java.sql.Connection conn))
                  (is (= "bluejdbc.connection.ProxyConnection"
                         (some-> conn class .getCanonicalName)))
                  (is (= [{:ONE 1}]
                         (jdbc/query conn "SELECT 1 AS one;")))
                  (when options
                    (is (= {:option? true}
                           (options/options conn))))))))))))
  (testing "Other parameters should be passed as Properties"
    (with-test-driver
      (with-open [conn (jdbc/connect! {:classname   `TestDriver
                                       :subprotocol "bluejdbc-test-driver"
                                       :subname     "wow"
                                       :x           100
                                       :y           true
                                       :z           "OK"})]
        (is (= "bluejdbc.connection.ProxyConnection"
               (some-> conn class .getCanonicalName)))
        (let [unwrapped (.unwrap conn MockConnection)]
          (is (= {:url        "jdbc:bluejdbc-test-driver:wow"
                  :properties {:x "100"
                               :y "true" :z "OK"}}
                 (into {} unwrapped)))))))
  (testing "Should throw Exception if subprotocol is missing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Can't create Connection .*: missing :subprotocol"
         (jdbc/connect! {:classname "org.h2.Driver"
                         :subname   "mem:bluejdbc_test;DB_CLOSE_DELAY=-1"}))))
  (testing "Should throw Exception if subname is missing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Can't create Connection .*: missing :subname"
         (jdbc/connect! {:classname   "org.h2.Driver"
                         :subprotocol "h2"})))))

(deftest named-connectable-test
  (testing "Should be able to create a named connectable; should be identical to using connectable directly"
    (with-test-driver
      (let [jdbc-url "jdbc:bluejdbc-test-driver://localhost:1337/my_db"]
        (jdbc/with-connection [conn jdbc-url]
          (let [unwrapped (.unwrap conn MockConnection)]
            (is (= {:url        "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                    :properties {}}
                   (into {} unwrapped)))))
        (jdbc/defmethod jdbc/named-connectable ::test-connection
          [_]
          "jdbc:bluejdbc-test-driver://localhost:1337/my_db")
        (jdbc/with-connection [conn ::test-connection]
          (let [unwrapped (.unwrap conn MockConnection)]
            (is (= {:url        "jdbc:bluejdbc-test-driver://localhost:1337/my_db"
                    :properties {}}
                   (into {} unwrapped)))))))))
