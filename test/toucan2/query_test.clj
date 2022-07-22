(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [toucan2.query :as query]
   [toucan2.test :as test]))

;; TODO -- not 100% sure it makes sense for Toucan to be doing the magic key transformations automatically here without
;; us even asking!

(deftest reducible-query-test
  (testing "raw SQL"
    (is (= [{:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56-00:00")}
            {:id 2, :name "Sam", :created-at (java.time.OffsetDateTime/parse "2019-01-11T23:56-00:00")}
            {:id 3, :name "Pam", :created-at (java.time.OffsetDateTime/parse "2020-01-01T21:56-00:00")}
            {:id 4, :name "Tam", :created-at (java.time.OffsetDateTime/parse "2020-05-25T19:56-00:00")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db "SELECT * FROM people ORDER BY id ASC;")))))
  (testing "SQL + params"
    (is (= [{:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db ["SELECT * FROM people WHERE id = ?" 1]))))))

(deftest query-test
  (let [expected [{:id 1, :created-at (java.time.LocalDateTime/parse "2016-12-31T16:00")}
                  {:id 2, :created-at (java.time.LocalDateTime/parse "2016-12-31T16:00")}
                  {:id 3, :created-at (java.time.LocalDateTime/parse "2016-12-31T16:00")}]]
    (testing "SQL"
      (query/query :toucan2.test/db "SELECT * FROM venues ORDER BY id ASC;"))
    (testing "HoneySQL"
      (is (= expected
             (query/query :toucan2.test/db {:select [:id :created_at], :from [:venues], :order-by [[:id :asc]]}))))))
