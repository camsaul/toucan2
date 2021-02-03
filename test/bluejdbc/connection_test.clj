(ns bluejdbc.connection-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [bluejdbc.util :as u]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [potemkin :as p])
  (:import java.sql.DriverManager))

(deftest driver-test
  (testing "Should be able to get a Driver"
    (let [pg-driver-class (Class/forName "org.postgresql.Driver")]
      (testing "from a Driver"
        (let [driver (.newInstance pg-driver-class)]
          (is (identical? driver
                          (jdbc/driver driver)))))

      (testing "from the Class"
        (is (instance? pg-driver-class (jdbc/driver pg-driver-class))))

      (testing "from String class name"
        (is (instance? pg-driver-class (jdbc/driver "org.postgresql.Driver"))))

      (testing "from String JDBC URL"
        (is (instance? pg-driver-class (jdbc/driver "jdbc:postgresql://localhost:5432/fake_db")))))))

(deftest connection-test
  (testing "Should be able to create a connection via a function"
    (jdbc/with-connection [conn test/connection]
      (is (instance? java.sql.Connection conn))))
  (testing "Should be able to create a named Connection"
    (jdbc/with-connection [conn (test/connection)]
      (is (instance? java.sql.Connection conn)))))

(p/defrecord+ ^:private MockConnection []
  java.sql.Connection

  java.lang.AutoCloseable
  (close [_])

  java.sql.Wrapper
  (unwrap [this interface]
    (when (instance? interface this)
      this)))

(p/defrecord+ ^:private TestDriver []
  java.sql.Driver
  (acceptsURL [_ url]
              (str/starts-with? url "jdbc:bluejdbc-test-driver:"))

  (connect [_ url properties]
           (assoc (MockConnection.)
                  :url        url
                  :properties (into {} (for [[k v] properties]
                                         [(keyword k) v])))))

(p/defrecord+ ^:private TestDriver2 []
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
                :options     {:connection/properties (u/->Properties {:user "cam", :password "cam"})}
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
          (jdbc/with-connection [conn url options]
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
            (jdbc/with-connection [conn m]
              (is (instance? java.sql.Connection conn))))))))
  (testing "Other parameters should be passed as Properties"
    (with-test-driver
      (jdbc/with-connection [conn {:classname   `TestDriver
                                   :subprotocol "bluejdbc-test-driver"
                                   :subname     "wow"
                                   :x           100
                                   :y           true
                                   :z           "OK"}]
        (let [unwrapped (.unwrap conn MockConnection)]
          (is (= {:url        "jdbc:bluejdbc-test-driver:wow"
                  :properties {:x "100"
                               :y "true" :z "OK"}}
                 (into {} unwrapped)))))))
  (testing "Should throw Exception if subprotocol is missing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Can't create Connection .*: missing :subprotocol"
         (jdbc/connection {:classname "org.h2.Driver"
                           :subname   "mem:bluejdbc_test;DB_CLOSE_DELAY=-1"}))))
  (testing "Should throw Exception if subname is missing"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Can't create Connection .*: missing :subname"
         (jdbc/connection {:classname   "org.h2.Driver"
                           :subprotocol "h2"})))))
