(ns toucan2.connection-test
  (:require
   [clojure.test :refer :all]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(deftest ^:parallel connection-string-protocol-test
  (doseq [[s expected] {"jdbc:postgres://localhost:5432/my_db" "jdbc"
                        "jdbc:" "jdbc"
                        "jdbc" nil
                        "" nil
                        nil nil}]
    (testing (pr-str `(conn/connection-string-protocol ~s))
      (is (= expected
             (conn/connection-string-protocol s))))))

(deftest with-connection-test
  (letfn [(test-current-connection-bound? []
            (testing "current connectable should be bound"
              (is (instance? java.sql.Connection current/*connectable*))))
          (test-connection [conn]
            (is (instance? java.sql.Connection conn))
            (test-current-connection-bound?))]
    (testing "Connection from keyword"
      (conn/with-connection [conn-from-keyword ::test/db]
        (test-connection conn-from-keyword)
        (testing "Connection from Connection"
          (conn/with-connection [conn-from-conn conn-from-keyword]
            (test-connection conn-from-conn)))))
    (testing "nil connectable = current connection"
      (testing "nil second arg"
        (binding [current/*connectable* ::test/db]
          (conn/with-connection [conn nil]
            (test-connection conn)))
        (testing "no second arg"
          (binding [current/*connectable* ::test/db]
            (conn/with-connection [conn]
              (test-connection conn))))))))

(deftest jdbc-spec-test
  (conn/with-connection [conn {:dbtype   "h2:mem"
                               :dbname   "spec_test"}]
    (is (instance? org.h2.jdbc.JdbcConnection conn))))
