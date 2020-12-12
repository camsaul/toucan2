(ns bluejdbc.table-aware-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]))

(deftest insert!-test
  (testing "insert!"
    (doseq [[description {:keys [f expected]}]
            {"with row maps"
             {:f        (fn [conn]
                          (jdbc/insert! conn :insert_test_table [{:id 1, :name "Cam"}
                                                                 {:id 2, :name "Sam"}]))
              :expected [{:id 1, :name "Cam"}
                         {:id 2, :name "Sam"}]}

             "with a single row map"
             {:f        (fn [conn]
                          (jdbc/insert! conn :insert_test_table {:id 1, :name "Cam"}))
              :expected [{:id 1, :name "Cam"}]}

             "with row vectors"
             {:f        (fn [conn]
                          (jdbc/insert! conn :insert_test_table [:id :name] [[1 "Cam"] [2 "Sam"]]))
              :expected [{:id 1, :name "Cam"}
                         {:id 2, :name "Sam"}]}}]
      (testing description
        (jdbc/with-connection [conn (test/connection)]
          (test/with-test-table conn :insert_test_table "(id INTEGER NOT NULL, name TEXT NOT NULL)"
            (is (= (count expected)
                   (f conn)))
            (is (= expected
                   (jdbc/query conn {:select   [:*]
                                     :from     [:insert_test_table]
                                     :order-by [[:id :asc]]})))))))))

(deftest insert-returning-keys!-test
  (jdbc/with-connection [conn (test/connection)]
    (test/with-test-table conn :returning_keys_test (format "(id %s PRIMARY KEY, name TEXT NOT NULL)" (test/autoincrement-type))
      (testing "If return-generated-keys is true, just return whatever the DB returns"
        (is (= (case (test/db-type)
                 ;; H2 only returns the keys that were actually generated
                 :h2    [{:id 1}]
                 :mysql [{:insert_id 1}]
                 [{:id 1, :name "Cam"}])
               (jdbc/insert-returning-keys! conn
                                            :returning_keys_test
                                            {:name "Cam"}
                                            {:statement/return-generated-keys true}))))
      (let [generated-key (case (test/db-type)
                            :h2    :ID
                            :mysql :insert_id
                            :id)]
        (testing "Should be able to specify *which* keys get returned"
          (is (= [2]
                 (jdbc/insert-returning-keys! conn
                                              :returning_keys_test
                                              {:name "Cam"}
                                              {:statement/return-generated-keys generated-key})))
          (is (= [{generated-key 3}]
                 (jdbc/insert-returning-keys! conn
                                              :returning_keys_test
                                              {:name "Cam"}
                                              {:statement/return-generated-keys [generated-key]}))))))))

(deftest conditions->where-clause-test
  (testing "nil"
    (is (= nil
           (table-aware/conditions->where-clause nil))))
  (testing "empty map"
    (is (= nil
           (table-aware/conditions->where-clause {}))))
  (testing "map"
    (is (= [:= :a 1]
           (table-aware/conditions->where-clause {:a 1})))
    (is (= [:and [:= :a 1] [:= :b 2]]
           (table-aware/conditions->where-clause {:a 1, :b 2})))
    (testing "value is a vector"
      (is (= [:> :a 1]
             (table-aware/conditions->where-clause {:a [:> 1]}))))
    (testing "value is a vector"
      (is (= [:between :a 1 100]
             (table-aware/conditions->where-clause {:a [:between 1 100]})))))
  (testing "HoneySQL-style vector"
    (is (= [:between :a 1 100]
           (table-aware/conditions->where-clause [:between :a 1 100])))))

(deftest update!-test
  (jdbc/with-connection [conn (test/connection)]
    (test/with-test-table conn :venues "(id INTEGER NOT NULL, price INTEGER NOT NULL, name TEXT NOT NULL, expensive BOOLEAN)"
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
                 (venues))))
        (testing "no rows affected"
          (is (= 0
                 (jdbc/update! conn :venues {:price 5} {:expensive true})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))))))

(deftest delete!-test
  (jdbc/with-connection [conn (test/connection)]
    (test/with-test-table conn :venues "(id INTEGER NOT NULL, price INTEGER NOT NULL, name TEXT NOT NULL)"
      (is (= 3
             (jdbc/insert! conn :venues nil [{:id 1, :price 1, :name "Cheap Burgers"}
                                             {:id 2, :price 2, :name "Cheap Pizza"}
                                             {:id 3, :price 4, :name "Expensive Sushi"}])))
      (letfn [(venues []
                (jdbc/query conn {:select   [:*]
                                  :from     [:venues]
                                  :order-by [[:id :asc]]}))]
        (testing "map conditions"
          (is (= 1
                 (jdbc/delete! conn :venues {:price 2})))
          (is (=
               [{:id 1, :price 1, :name "Cheap Burgers"}
                {:id 3, :price 4, :name "Expensive Sushi"}]
               (venues))))
        (testing "HoneySQL-style conditions"
          (is (= 1
                 (jdbc/delete! conn :venues [:= :price 4])))
          (is (=
               [{:id 1, :price 1, :name "Cheap Burgers"}]
               (venues))))
        (testing "No conditions"
          (is (= 1
                 (jdbc/delete! conn :venues nil)))
          (is (= []
                 (venues))))))))
