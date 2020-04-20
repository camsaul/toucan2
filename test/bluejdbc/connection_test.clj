(ns bluejdbc.connection-test
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.options :as options]
            [bluejdbc.test :as t]
            [clojure.test :refer :all]))

(def ^:private mock-connection
  (reify
    java.sql.Connection
    java.lang.AutoCloseable
    (close [_])))

(defn- test-driver []
  (reify java.sql.Driver
    (connect [_ url properties]
      mock-connection)))

;; TODO -- details should be set by env var
(deftest connection-from-url-test
  (t/only :postgresql
    (let [url (t/jdbc-url)]
      (testing "Should fail without username/password"
        (is (thrown?
             Exception
             (conn/with-connection [conn url]
               (.prepareStatement conn "SELECT * FROM user;")))))

      (testing "should be able to create a Connection from a JDBC connection string"
        (conn/with-connection [conn (str url "?user=cam&password=cam")]
          (is (instance? java.sql.Connection conn)))

        (testing "with user and password keys"
          (conn/with-connection [conn url {:connection/user "cam", :connection/password "cam"}]
            (is (instance? java.sql.Connection conn))))

        (testing "with Properties"
          (conn/with-connection [conn url {:connection/properties (options/->Properties {:user "cam", :password "cam"})}]
            (is (instance? java.sql.Connection conn))))

        (testing "with :properties map; should automatically convert to Properties"
          (conn/with-connection [conn url {:connection/properties {:user "cam", :password "cam"}}]
            (is (instance? java.sql.Connection conn))))

        (testing "with a specific driver"
          (conn/with-connection [conn url {:connection/driver (test-driver)}]
            (is (identical? mock-connection conn))))))))
