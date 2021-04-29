(ns bluejdbc.query-test
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(deftest reducible-query-test
  (is (= [{:count 4}]
         (let [query (query/reducible-query :test/postgres "SELECT count(*) FROM people;")]
           (into [] (map query/realize-row) query))))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= [{:count 4}]
             (into [] (map #(select-keys % [:count])) (query/reducible-query "SELECT count(*) FROM people;"))))))

  (testing "eductions"
    (is (= [{:id 2, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}]
           (into
            []
            (map query/realize-row)
            (eduction
             (comp (map #(update % :id inc))
                   (take 1))
             (query/reducible-query :test/postgres "SELECT * FROM people;")))))))

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  "SELECT count(*) FROM people;")

(deftest query-test
  (is (= [{:count 4}]
         (query/query :test/postgres "SELECT count(*) FROM people;")))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= [{:count 4}]
             (query/query "SELECT count(*) FROM people;")))))

  (testing "HoneySQL query"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           (query/query :test/postgres {:select [:*], :from [:people]}))))

  (testing "named query"
    (is (= [{:count 4}]
           (query/query :test/postgres ::named-query)))))

(deftest query-one-test
  (is (= {:count 4}
         (query/query-one :test/postgres "SELECT count(*) FROM people")))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= {:count 4}
             (query/query-one "SELECT count(*) FROM people;"))))))

(m/defmethod conn/connection* ::not-even-jdbc
  [_ options]
  {:connection nil
   :new?       false
   :options    options})

(m/defmethod query/reducible-query* [::not-even-jdbc :default :default]
  [connectable _ k options]
  (reify
    clojure.lang.IReduceInit
    (reduce [_ rf init]
      (reduce rf init [{k 1} {k 2} {k 3}]))))

(deftest wow-dont-even-need-to-use-jdbc-test
  (is (= [{:a 1} {:a 2} {:a 3}]
         (query/query ::not-even-jdbc :a))))
