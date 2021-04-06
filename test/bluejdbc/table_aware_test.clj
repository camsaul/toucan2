(ns bluejdbc.table-aware-test
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.core :as bluejdbc]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

#_(use-fixtures :each (fn [thunk]
                      (test/with-every-test-connectable [_]
                        (thunk))))

(deftest select-test
  (test/with-every-test-connectable [_]
    (test/with-test-data [test/*connectable* :people]
      (is (= (case (keyword (name test/*connectable*))
               :h2
               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T16:56-07:00")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T15:56-08:00")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T13:56-08:00")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T12:56-07:00")}]
               :postgres

               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}])
             (bluejdbc/select :people))))))

(deftest insert!-test
  (testing "insert!"
    (doseq [[description {:keys [f expected]}]
            {"with row maps"
             {:f        (fn [conn]
                          (bluejdbc/insert! conn :insert_test_table nil [{:id 1, :name "Cam"}
                                                                         {:id 2, :name "Sam"}]))
              :expected [{:id 1, :name "Cam"}
                         {:id 2, :name "Sam"}]}

             "with a single row map"
             {:f        (fn [conn]
                          (bluejdbc/insert! conn :insert_test_table nil {:id 1, :name "Cam"}))
              :expected [{:id 1, :name "Cam"}]}

             "with row vectors"
             {:f        (fn [conn]
                          (bluejdbc/insert! conn :insert_test_table [:id :name] [[1 "Cam"] [2 "Sam"]]))
              :expected [{:id 1, :name "Cam"}
                         {:id 2, :name "Sam"}]}}]
      (testing description
        (bluejdbc/with-connection [conn conn/*current-connectable*]
          (test/with-test-data [conn [{:name         "insert_test_table"
                                       :columns      [{:name "id", :class Long, :not-null? true}
                                                      {:name "name", :class String, :not-null? true}]
                                       :primary-keys ["id"]}]]
            (is (= (count expected)
                   (f conn)))
            (is (= expected
                   (bluejdbc/query conn {:select   [:*]
                                         :from     [:insert_test_table]
                                         :order-by [[:id :asc]]})))))))))

(deftest insert-returning-keys!-test
  (bluejdbc/with-connection [conn conn/*current-connectable*]
    (test/with-test-data [conn [{:name         "returning_keys_test"
                                 :columns      [{:name "id", :class ::test/autoincrement}
                                                {:name "name", :class String, :not-null? true}]
                                 :primary-keys ["id"]}]]
      (testing "If return-generated-keys is true, just return whatever the DB returns"
        (is (= (case (keyword (name &conn))
                 ;; H2 only returns the keys that were actually generated
                 :h2    [{:id 1}]
                 :mysql [{:insert_id 1}]
                 [{:id 1, :name "Cam"}])
               (bluejdbc/insert-returning-keys! conn
                                                :returning_keys_test
                                                nil
                                                {:name "Cam"}))))
      (let [generated-key (case (keyword (name &conn))
                            :h2    :id
                            :mysql :insert_id
                            :id)]
        (testing "Should be able to specify *which* keys get returned"
          (is (= [2]
                 (bluejdbc/insert-returning-keys! conn
                                                  :returning_keys_test
                                                  nil
                                                  {:name "Cam"}
                                                  {:statement/return-generated-keys generated-key})))
          (is (= [{generated-key 3}]
                 (bluejdbc/insert-returning-keys! conn
                                                  :returning_keys_test
                                                  nil
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
  (bluejdbc/with-connection [conn conn/*current-connectable*]
    (test/with-test-data [conn [{:name    :venues
                                 :columns [{:name "id", :class Long, :not-null? true}
                                           {:name "price", :class Long, :not-null? true}
                                           {:name "name", :class String, :not-null? true}
                                           {:name "expensive", :class Boolean}]
                                 :primary-keys ["id"]}]]
      (is (= 3
             (bluejdbc/insert! conn :venues nil [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                                                 {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                                                 {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}])))
      (letfn [(venues []
                (bluejdbc/query conn {:select   [:*]
                                      :from     [:venues]
                                      :order-by [[:id :asc]]}))]
        (testing "Basic field = x condition"
          (is (= 1
                 (bluejdbc/update! conn :venues {:price 4} {:expensive true})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                 (venues))))
        (testing "fancy field = [...] conditions"
          (is (= 2
                 (bluejdbc/update! conn :venues {:price [:< 3]} {:expensive false})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                 (venues)))
          (testing "More that 1 arg"
            (is (= 2
                   (bluejdbc/update! conn :venues {:price [:between 1 3]} {:expensive nil})))
            (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                    {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                    {:id 3, :price 4, :name "Expensive Sushi", :expensive true}]
                   (venues)))))
        (testing "no conditions"
          (is (= 3
                 (bluejdbc/update! conn :venues nil {:expensive nil})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive nil}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive nil}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))
        (testing "conditions as HoneySQL clause"
          (is (= 2
                 (bluejdbc/update! conn :venues [:<= :price 3] {:expensive false})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))
        (testing "no rows affected"
          (is (= 0
                 (bluejdbc/update! conn :venues {:price 5} {:expensive true})))
          (is (= [{:id 1, :price 1, :name "Cheap Burgers", :expensive false}
                  {:id 2, :price 2, :name "Cheap Pizza", :expensive false}
                  {:id 3, :price 4, :name "Expensive Sushi", :expensive nil}]
                 (venues))))))))

(deftest delete!-test
  (bluejdbc/with-connection [conn conn/*current-connectable*]
    (test/with-test-data [conn [{:name    :venues
                                 :columns [{:name "id", :class Long, :not-null? true}
                                           {:name "price", :class Long, :not-null? true}
                                           {:name "name", :class String, :not-null? true}]
                                 :primary-keys ["id"]}]]
      (is (= 3
             (bluejdbc/insert! conn :venues nil [{:id 1, :price 1, :name "Cheap Burgers"}
                                                 {:id 2, :price 2, :name "Cheap Pizza"}
                                                 {:id 3, :price 4, :name "Expensive Sushi"}])))
      (letfn [(venues []
                (bluejdbc/query conn {:select   [:*]
                                      :from     [:venues]
                                      :order-by [[:id :asc]]}))]
        (testing "map conditions"
          (is (= 1
                 (bluejdbc/delete! conn :venues {:price 2})))
          (is (=
               [{:id 1, :price 1, :name "Cheap Burgers"}
                {:id 3, :price 4, :name "Expensive Sushi"}]
               (venues))))
        (testing "HoneySQL-style conditions"
          (is (= 1
                 (bluejdbc/delete! conn :venues [:= :price 4])))
          (is (=
               [{:id 1, :price 1, :name "Cheap Burgers"}]
               (venues))))
        (testing "No conditions"
          (is (= 1
                 (bluejdbc/delete! conn :venues nil)))
          (is (= []
                 (venues))))))))
