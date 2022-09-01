(ns toucan2.connection-test
  (:require
   [clojure.test :refer :all]
   [toucan2.connection :as conn]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.update :as update]))

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

(defn- test-current-connection-bound? []
  (testing (format "current connectable should be bound. *current-connectable* = %s"
                   (pr-str conn/*current-connectable*))
    (is (instance? java.sql.Connection conn/*current-connectable*))))

(defn- test-connection [conn]
  (is (instance? java.sql.Connection conn))
  (test-current-connection-bound?))

(deftest ^:parallel with-connection-test
  (testing "Connection from keyword"
    (conn/with-connection [conn-from-keyword ::test/db]
      (test-connection conn-from-keyword)
      (testing "Connection from Connection"
        (conn/with-connection [conn-from-conn conn-from-keyword]
          (test-connection conn-from-conn)))))
  (testing "nil connectable = current connection"
    (testing "nil second arg"
      (binding [conn/*current-connectable* ::test/db]
        (conn/with-connection [conn nil]
          (test-connection conn)))
      (testing "no second arg"
        (binding [conn/*current-connectable* ::test/db]
          (conn/with-connection [conn]
            (test-connection conn)))
        (testing "no bindings"
          (binding [conn/*current-connectable* ::test/db]
            (conn/with-connection []
              (is (instance? java.sql.Connection conn/*current-connectable*))
              (test-connection conn/*current-connectable*))
            (testing "nil instead of bindings vector"
              (conn/with-connection nil
                (is (instance? java.sql.Connection conn/*current-connectable*))
                (test-connection conn/*current-connectable*)))))))))

(deftest transaction-test
  (testing "nil connectable = current connection"
    (testing "nil second arg"
      (binding [conn/*current-connectable* ::test/db]
        (conn/with-transaction [conn nil]
          (test-connection conn)))
      (testing "no second arg"
        (binding [conn/*current-connectable* ::test/db]
          (conn/with-transaction [conn]
            (test-connection conn)))
        (testing "no bindings"
          (binding [conn/*current-connectable* ::test/db]
            (conn/with-transaction []
              (is (instance? java.sql.Connection conn/*current-connectable*))
              (test-connection conn/*current-connectable*))
            (testing "nil instead of bindings vector"
              (conn/with-transaction nil
                (is (instance? java.sql.Connection conn/*current-connectable*))
                (test-connection conn/*current-connectable*))))))))
  (test/with-discarded-table-changes :venues
    (is (thrown?
         clojure.lang.ExceptionInfo
         (conn/with-transaction [_conn]
           (update/update! ::test/venues 1 {:name "Tin Vietnamese"})
           (is (= "Tin Vietnamese"
                  (select/select-one-fn :name ::test/venues 1)))
           (throw (ex-cause "OOPS")))
         (is (= "Tin Vietnamese"
                (select/select-one-fn :name ::test/venues 1)))))))

(deftest ^:parallel jdbc-spec-test
  (conn/with-connection [conn {:dbtype   "h2:mem"
                               :dbname   "spec_test"}]
    (is (instance? org.h2.jdbc.JdbcConnection conn))))
