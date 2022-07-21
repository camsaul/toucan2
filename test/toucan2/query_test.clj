(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [toucan2.query :as query]
   [toucan2.test :as test]))

(deftest reducible-query-test
  (testing "raw SQL"
    (is (= [{:id 1, :name "Cam", :created-at #inst "2020-04-21T23:56:00.000000000-00:00"}
            {:id 2, :name "Sam", :created-at #inst "2019-01-11T23:56:00.000000000-00:00"}
            {:id 3, :name "Pam", :created-at #inst "2020-01-01T21:56:00.000000000-00:00"}
            {:id 4, :name "Tam", :created-at #inst "2020-05-25T19:56:00.000000000-00:00"}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db "SELECT * FROM people ORDER BY id ASC;")))))
  (testing "SQL + params"
    (is (= [{:id 1, :name "Cam", :created-at #inst "2020-04-21T23:56:00.000000000-00:00"}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db ["SELECT * FROM people WHERE id = ?" 1]))))))
