(ns bluejdbc.driver-test
  (:require [bluejdbc.driver :as driver]
            [bluejdbc.test :as t]
            [clojure.test :refer :all]))

(deftest driver-test
  (testing "Should be able to get a Driver"
    (t/only :postgres
      (let [pg-driver-class (Class/forName "org.postgresql.Driver")]
        (testing "from a Driver"
          (let [driver (.newInstance pg-driver-class)]
            (is (identical? driver
                            (driver/driver driver)))))

        (testing "from the Class"
          (is (instance? pg-driver-class (driver/driver pg-driver-class))))

        (testing "from String class name"
          (is (instance? pg-driver-class (driver/driver "org.postgresql.Driver"))))

        (testing "from String JDBC URL"
          (is (instance? pg-driver-class (driver/driver "jdbc:postgresql://localhost:5432/fake_db"))))))))
