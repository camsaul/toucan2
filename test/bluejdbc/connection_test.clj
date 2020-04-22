(ns bluejdbc.connection-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.options :as options]
            [bluejdbc.protocols :as protocols]
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
               (.getCanonicalName (class conn))))
        (is (= "bluejdbc.statement.ProxyPreparedStatement"
               (.getCanonicalName (class stmt))))

        (testing "options should be passed along"
          (is (= {:x :y}
                 (select-keys (protocols/options stmt) [:x]))))))))

(p.types/defrecord+ ^:private MockConnection3 []
  java.sql.Connection

  java.lang.AutoCloseable
  (close [_])

  java.sql.Wrapper
  (unwrap [this interface]
    (when (instance? interface this)
      this)))

(p.types/defrecord+ ^:private TestDriver3 []
  java.sql.Driver
  (acceptsURL [_ url]
    (str/starts-with? url "jdbc:bluejdbc-test-driver:"))

  (connect [_ url properties]
    (assoc (MockConnection3.)
           :url        url
           :properties (into {} (for [[k v] properties]
                                  [(keyword k) v])))))

(p.types/defrecord+ ^:private TestDriver4 []
  java.sql.Driver
  (acceptsURL [_ url]
    (str/starts-with? url "jdbc:bluejdbc-test-driver:"))

  (connect [_ url properties]
    (assoc (MockConnection3.)
           :url        url
           :properties (into {} (for [[k v] properties]
                                  [(keyword k) v]))
           :driver TestDriver4)))

(defn- do-with-registered-driver
  ([thunk]
   (do-with-registered-driver (TestDriver3.) thunk))

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
          (is (= "bluejdbc.connection_test.TestDriver3"
                 (.getCanonicalName (class driver))))

          (is (= true
                 (.acceptsURL driver mock-url))))

        (with-open [conn (DriverManager/getConnection mock-url)]
          (is (= "bluejdbc.connection_test.MockConnection3"
                 (.getCanonicalName (class conn)))))))))

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
                :options     {:connection/driver (TestDriver4.)}
                :expected    {:properties {}
                              :driver     TestDriver4}}]]
        (testing description
          (with-open [conn (jdbc/connect! url options)]
            (is (= "bluejdbc.connection.ProxyConnection"
                   (.getCanonicalName (class conn))))
            (testing "Options should be set; :connection/type should be added"
              (is (= (assoc options :connection/type :bluejdbc-test-driver)
                     (protocols/options conn))))

            (let [unwrapped (.unwrap conn MockConnection3)]
              (testing "Should be able to unwrap"
                (is (= "bluejdbc.connection_test.MockConnection3"
                       (.getCanonicalName (class unwrapped))))

                (is (= (merge {:url url}
                              expected)
                       (into {} unwrapped)))))))))))
