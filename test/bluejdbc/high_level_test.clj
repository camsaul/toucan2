(ns bluejdbc.high-level-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.high-level :as high-level]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(defn- wacky-row-xform [_]
  (fn [rf]
    (fn
      ([] (rf))
      ([acc] (rf acc))
      ([acc row] (rf acc (cons :row row))))))

(deftest query-test
  (let [t   (t/offset-date-time "2020-04-15T07:04:02.465161Z")
        sql "SELECT ? AS t;"]
    (jdbc/with-connection [conn (test/jdbc-url)]
      (testing "(query"
        (testing "conn query)"
          (is (= [{:t t}]
                 (jdbc/query conn [sql t]))))

        (testing "stmt)"
          (is (= [{:t t}]
                 (jdbc/with-prepared-statement [stmt conn [sql t]]
                   (jdbc/query stmt)))))

        (doseq [[options expected] {nil
                                    [{:t t}]

                                    {:results/xform nil}
                                    [[t]]

                                    {:results/xform wacky-row-xform}
                                    [[:row t]]}]
          (testing (format "conn query %s)" (pr-str options))
            (is (= expected
                   (jdbc/query conn [sql t] options))))

          (jdbc/with-prepared-statement [stmt conn [sql t]]
            (testing (format "stmt %s)" (pr-str options))
              (is (= expected
                     (jdbc/query stmt options))))

            (testing (format "conn stmt %s)" (pr-str options))
              (is (= expected
                     (jdbc/query conn stmt options))))))))))

(deftest query-one-test
  (is (= {:abc "abc"}
         (jdbc/query-one (test/jdbc-url) "SELECT 'abc' AS abc;")))

  (testing "With a custom results xform"
    (is (= ["abc"]
           (jdbc/query-one (test/jdbc-url) "SELECT 'abc' AS abc;" {:results/xform nil}))))

  (testing "query that returns no columns should still work"
    (is (= {}
           (jdbc/query-one (test/jdbc-url) "SELECT;")))
    (is (= []
           (jdbc/query-one (test/jdbc-url) "SELECT;" {:results/xform nil})))))

(deftest execute-test
  (testing "execute!"
    (jdbc/with-connection [conn (test/jdbc-url)]
      (try
        (is (= 0
               (jdbc/execute! conn "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
        (is (= []
               (jdbc/query conn "SELECT * FROM execute_test_table;")))
        (finally
          (jdbc/execute! conn "DROP TABLE IF EXISTS execute_test_table;"))))))

(defmacro ^:private with-test-table {:style/indent 3} [conn table-name fields & body]
  `(let [conn# ~conn]
     (try
       (jdbc/execute! conn# ~(format "DROP TABLE IF EXISTS %s;" (name table-name)))
       (is (= 0
              (jdbc/execute! conn# ~(format "CREATE TABLE %s %s;" (name table-name) fields))))
       ~@body
       (finally
         (jdbc/execute! conn# ~(format "DROP TABLE IF EXISTS %s;" (name table-name)))))))

(deftest insert-test
  (testing "insert!"
    (doseq [[description f] {"with row maps"
                             (fn [conn]
                               (jdbc/insert! conn :execute_test_table [{:id 1, :name "Cam"}
                                                                       {:id 2, :name "Sam"}]))

                             "with row vectors"
                             (fn [conn]
                               (jdbc/insert! conn :execute_test_table [:id :name] [[1 "Cam"] [2 "Sam"]]))}]
      (testing description
        (jdbc/with-connection [conn (test/jdbc-url)]
          (with-test-table conn :execute_test_table "(id INTEGER NOT NULL, name TEXT NOT NULL)"
            (is (= 2
                   (f conn)))
            (is (= [{:id 1, :name "Cam"}
                    {:id 2, :name "Sam"}]
                   (jdbc/query conn {:select   [:*]
                                     :from     [:execute_test_table]
                                     :order-by [[:id :asc]]})))))))))

(deftest conditions->where-clause-test
  (testing "nil"
    (is (= nil
           (high-level/conditions->where-clause nil))))
  (testing "empty map"
    (is (= nil
           (high-level/conditions->where-clause {}))))
  (testing "map"
    (is (= [:= :a 1]
           (high-level/conditions->where-clause {:a 1})))
    (is (= [:and [:= :a 1] [:= :b 2]]
           (high-level/conditions->where-clause {:a 1, :b 2})))
    (testing "value is a vector"
      (is (= [:> :a 1]
             (high-level/conditions->where-clause {:a [:> 1]}))))
    (testing "value is a vector"
      (is (= [:between :a 1 100]
             (high-level/conditions->where-clause {:a [:between 1 100]})))))
  (testing "HoneySQL-style vector"
    (is (= [:between :a 1 100]
           (high-level/conditions->where-clause [:between :a 1 100])))))

(deftest update!-test
  (jdbc/with-connection [conn (test/jdbc-url)]
    (with-test-table conn :venues "(id INTEGER NOT NULL, price INTEGER NOT NULL, name TEXT NOT NULL, expensive BOOLEAN)"
      (is (= 3
             (jdbc/insert! conn :venues [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                                         {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                                         {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}])))
      (letfn [(venues []
                (jdbc/query conn {:select   [:*]
                                  :from     [:venues]
                                  :order-by [[:id :asc]]}))]
        (testing "Basic field = x condition"
          (is (= 1
                 (jdbc/update! conn :venues {:price 4} {:expensive true})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                 (venues))))
        (testing "fancy field = [...] conditions"
          (is (= 2
                 (jdbc/update! conn :venues {:price [:< 3]} {:expensive false})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                 (venues)))
          (testing "More that 1 arg"
            (is (= 2
                   (jdbc/update! conn :venues {:price [:between 1 3]} {:expensive nil})))
            (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                    {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                    {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                   (venues)))))
        (testing "no conditions"
          (is (= 3
                 (jdbc/update! conn :venues nil {:expensive nil})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))
        (testing "conditions as HoneySQL clause"
          (is (= 2
                 (jdbc/update! conn :venues [:<= :price 3] {:expensive false})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))))))

(defn- transaction-test-names [conn]
  (jdbc/query conn "SELECT * FROM transaction_test_table ORDER BY name ASC;" {:results/xform nil}))

(deftest transaction-test
  (jdbc/with-connection [conn (test/jdbc-url)]
    (with-test-table conn :transaction_test_table "(name TEXT NOT NULL)"
      (testing "Test that `transaction` rolls back changes"
        (let [inserted-rows? (atom false)]
          (try
            (jdbc/transaction conn
              (jdbc/insert! conn :transaction_test_table [{:name "Cam"}])
              (is (= [["Cam"]]
                     (transaction-test-names conn)))
              (reset! inserted-rows? true)
              (throw (ex-info "Whoops!" {})))
            (catch Throwable e
              (is (= "Whoops!"
                     (.getMessage e))
                  "expected exception should have been thrown")))
          (is (= true
                 @inserted-rows?)
              "Rows should have been inserted"))
        (is (= []
               (transaction-test-names conn)))))))

(deftest nested-transaction-test
  (jdbc/with-connection [conn (test/jdbc-url)]
    (with-test-table conn :transaction_test_table "(name TEXT NOT NULL)"
      (testing "Test that we can nest transactions"
        (let [inserted-rows? (atom false)]
          (try
            (jdbc/transaction conn
              (jdbc/insert! conn :transaction_test_table [{:name "Cam"}])
              (try
                (jdbc/transaction conn
                  (jdbc/insert! conn :transaction_test_table [{:name "Sam"}])
                  (is (= [["Cam"] ["Sam"]]
                         (transaction-test-names conn)))
                  (reset! inserted-rows? true)
                  (throw (ex-info "Whoops!" {})))
                (catch Throwable e
                  (is (= "Whoops!"
                         (.getMessage e)))))
              (is (= [["Cam"]]
                     (transaction-test-names conn)))
              (throw (ex-info "Oh no!" {})))
            (catch Throwable e
              (is (= "Oh no!"
                     (.getMessage e)))))
          (is (= true
                 @inserted-rows?)))
        (is (= []
               (transaction-test-names conn)))))))
