(ns bluejdbc.table-aware-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(use-fixtures :once test/do-with-test-data)

(defn- test-people-instances? [results]
  (testing "All results should be :people instances"
    (is (every? (partial = :people) (map instance/table results)))))

(deftest query-as-test
  (let [results (table-aware/query-as :test/postgres :people {:select [:*], :from [:people]} nil)]
    (test-people-instances? results)
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           results))))

(deftest basic-select-test
  (doseq [[message thunk] {"should be able to do a HoneySQL query"
                           #(table-aware/basic-select :test/postgres :people {:select [:id]} nil)

                           "should be able to do a plain SQL query"
                           #(table-aware/basic-select :test/postgres :people "SELECT id FROM people" nil)}]
    (testing message
      (let [results (thunk)]
        (test-people-instances? results)
        (is (= [{:id 1}
                {:id 2}
                {:id 3}
                {:id 4}]
               results))))))
