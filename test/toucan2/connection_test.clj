(ns toucan2.connection-test
  (:require
   [clojure.test :refer :all]
   [toucan2.connection :as conn]))

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
  (conn/with-connection [conn-from-keyword ::test/db]
    (testing "Connection from keyword"
      (is (instance? java.sql.Connection conn-from-keyword)))
    (conn/with-connection [conn-from-conn conn-from-keyword]
      (testing "Connection from Connection"
        (is (instance? java.sql.Connection conn-from-conn))))))

(deftest jdbc-spec-test
  (conn/with-connection [conn {:dbtype   "h2:mem"
                               :dbname   "spec_test"}]
    (is (instance? org.h2.jdbc.JdbcConnection conn))))
