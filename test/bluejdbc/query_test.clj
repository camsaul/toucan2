(ns bluejdbc.query-test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as bluejdbc]
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
  (test/with-every-test-connectable [connectable]
    (is (= [{:one 1}]
           (bluejdbc/query-all connectable "SELECT 1 AS \"one\";")))
    (testing "HoneySQL form"
      (is (= [{:one 1}]
             (bluejdbc/query-all connectable {:select [[1 :one]]}))))))

(deftest query-test
  (test/with-every-test-connectable [connectable]
    (let [t   (t/offset-date-time "2020-04-15T07:04:02.465161Z")
          sql "SELECT ? AS \"t\";"]
      (testing "(query"
        (testing "conn query)"
          (is (= [{:t t}]
                 (bluejdbc/query-all connectable [sql t]))))

        (doseq [[options expected] {nil
                                    [{:t t}]

                                    {:results/xform nil}
                                    [[t]]

                                    {:results/xform wacky-row-xform}
                                    [[:row t]]}]
          (testing (format "conn query %s)" (pr-str options))
            (is (= expected
                   (bluejdbc/query-all connectable [sql t] options)))))))))

(deftest query-one-test
  (test/with-every-test-connectable [connectable]
    (is (= {:abc "abc"}
           (bluejdbc/query-one connectable "SELECT 'abc' AS \"abc\";")))

    (testing "With a custom results xform"
      (is (= ["abc"]
             (bluejdbc/query-one connectable "SELECT 'abc' AS \"abc\";" {:results/xform nil}))))

    ;; this doesn't work on MySQL
    (bluejdbc/with-connection [conn connectable]
      (when-not (#{:mysql} (keyword (name connectable)))
        (testing "query that returns no columns should still work"
          (is (= nil
                 (bluejdbc/query-one connectable "SELECT;")))
          (is (= nil
                 (bluejdbc/query-one connectable "SELECT;" {:results/xform nil}))))))))

(deftest execute!-test
  (testing "execute!"
    (test/with-every-test-connectable [connectable]
      (try
        (is (= 0
               (bluejdbc/execute! connectable "CREATE TABLE \"execute_test_table\" (\"id\" INTEGER NOT NULL);")))
        (is (= []
               (bluejdbc/query connectable "SELECT * FROM \"execute_test_table\";")))
        (finally
          (bluejdbc/execute! connectable "DROP TABLE IF EXISTS \"execute_test_table\";"))))))

(defn- transaction-test-names [connectable]
  (bluejdbc/query connectable "SELECT * FROM \"transaction_test_table\" ORDER BY \"name\" ASC;" {:results/xform nil}))

(def ^:private transaction-test-data
  [{:name    "transaction_test_table"
    :columns [{:name "name", :class String, :not-null? true}]}])

(deftest transaction-test
  (test/with-every-test-connectable [connectable]
    (test/with-test-data [connectable transaction-test-data]
      (testing "Test that `transaction` rolls back changes"
        (let [inserted-rows? (atom false)]
          (binding [connection/*include-connection-info-in-exceptions* false]
            (try
              (bluejdbc/transaction [conn connectable]
                (bluejdbc/insert! connectable :transaction_test_table nil [{:name "Cam"}])
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
               (transaction-test-names connectable)))))))

(deftest nested-transaction-test
  (test/with-every-test-connectable [connectable]
    (test/with-test-data [connectable transaction-test-data]
      (binding [connection/*include-connection-info-in-exceptions* false]
        (testing "Test that we can nest transactions"
          (let [inserted-rows? (atom false)]
            (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo
                 #"Oh no!"
                 (bluejdbc/transaction [conn connectable]
                   (bluejdbc/insert! connectable :transaction_test_table nil [{:name "Cam"}])
                   (is (thrown-with-msg?
                        clojure.lang.ExceptionInfo
                        #"Whoops!"
                        (bluejdbc/transaction [conn connectable]
                          (bluejdbc/insert! connectable :transaction_test_table nil [{:name "Sam"}])
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
                 (transaction-test-names connectable))))))))
