(ns bluejdbc.table-aware-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.table-aware :as table-aware]
            [clojure.test :refer :all]
            [java-time :as t]))

(deftest query-as-test
  (let [results (table-aware/query-as :test/postgres :people {:select [:*], :from [:people]} nil)]
    (is (every? (partial = :people) (map instance/table results)))
    (is (= [(instance/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")})
            (instance/instance :people {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")})
            (instance/instance :people {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")})
            (instance/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")})]
           results))))
