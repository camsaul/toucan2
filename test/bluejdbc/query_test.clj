(ns bluejdbc.query-test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(defn- wacky-row-xform [_]
  (fn [rf]
    (fn
      ([] (rf))
      ([acc] (rf acc))
      ([acc row] (rf acc (cons :row row))))))

(deftest basic-query-test
  (is (= [{:one 1}]
         (jdbc/query test/connection "SELECT 1 AS one;")))
  (testing "HoneySQL form"
    (is (= [{:one 1}]
           (jdbc/query test/connection {:select [[1 :one]]})))))

(deftest query-test
  (let [t   (t/offset-date-time "2020-04-15T07:04:02.465161Z")
        sql "SELECT ? AS t;"]
    (jdbc/with-connection [conn (test/connection)]
      (testing "(query"
        (testing "conn query)"
          (is (= [{:t t}]
                 (jdbc/query conn [sql t]))))

        (doseq [[options expected] {nil
                                    [{:t t}]

                                    {:results/xform nil}
                                    [[t]]

                                    {:results/xform wacky-row-xform}
                                    [[:row t]]}]
          (testing (format "conn query %s)" (pr-str options))
            (is (= expected
                   (jdbc/query conn [sql t] options)))))))))

(deftest query-one-test
  (is (= {:abc "abc"}
         (jdbc/query-one (test/connection) "SELECT 'abc' AS abc;")))

  (testing "With a custom results xform"
    (is (= ["abc"]
           (jdbc/query-one (test/connection) "SELECT 'abc' AS abc;" {:results/xform nil}))))

  ;; this doesn't work on MySQL
  (when-not (#{:mysql} (connection/db-type (test/connection)))
    (testing "query that returns no columns should still work"
      (is (= nil
             (jdbc/query-one (test/connection) "SELECT;")))
      (is (= nil
             (jdbc/query-one (test/connection) "SELECT;" {:results/xform nil}))))))

(deftest execute!-test
  (testing "execute!"
    (jdbc/with-connection [conn (test/connection)]
      (try
        (is (= 0
               (jdbc/execute! conn "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
        (is (= []
               (jdbc/query conn "SELECT * FROM execute_test_table;")))
        (finally
          (jdbc/execute! conn "DROP TABLE IF EXISTS execute_test_table;"))))))

(defn- transaction-test-names [conn]
  (jdbc/query conn "SELECT * FROM transaction_test_table ORDER BY name ASC;" {:results/xform nil}))

(def ^:private transaction-test-data
  [{:name    "transaction_test_table"
    :columns [{:name "name", :class String, :not-null? true}]}])

(deftest transaction-test
  (jdbc/with-connection [conn (test/connection)]
    (test/with-test-data [conn transaction-test-data]
      (testing "Test that `transaction` rolls back changes"
        (let [inserted-rows? (atom false)]
          (binding [connection/*include-connection-info-in-exceptions* false]
            (try
              (jdbc/transaction conn
                (jdbc/insert! conn :transaction_test_table nil [{:name "Cam"}])
                (is (= [["Cam"]]
                       (transaction-test-names conn)))
                (reset! inserted-rows? true)
                (throw (ex-info "Whoops!" {})))
              (catch Throwable e
                (is (= "Whoops!"
                       (.getMessage e))
                    "expected exception should have been thrown"))))
          (is (= true
                 @inserted-rows?)
              "Rows should have been inserted"))
        (is (= []
               (transaction-test-names conn)))))))

(deftest nested-transaction-test
  (jdbc/with-connection [conn (test/connection)]
    (test/with-test-data [conn transaction-test-data]
      (binding [connection/*include-connection-info-in-exceptions* false]
        (testing "Test that we can nest transactions"
          (let [inserted-rows? (atom false)]
            (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo
                 #"Oh no!"
                 (jdbc/transaction conn
                   (jdbc/insert! conn :transaction_test_table nil [{:name "Cam"}])
                   (is (thrown-with-msg?
                        clojure.lang.ExceptionInfo
                        #"Whoops!"
                        (jdbc/transaction conn
                          (jdbc/insert! conn :transaction_test_table nil [{:name "Sam"}])
                          (is (= [["Cam"] ["Sam"]]
                                 (transaction-test-names conn)))
                          (reset! inserted-rows? true)
                          (throw (ex-info "Whoops!" {})))))
                   (is (= [["Cam"]]
                          (transaction-test-names conn)))
                   (throw (ex-info "Oh no!" {})))))
            (is (= true
                   @inserted-rows?)
                "Has inserted rows?"))
          (is (= []
                 (transaction-test-names conn))))))))
