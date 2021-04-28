(ns bluejdbc.query-test
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.query :as query]
            [bluejdbc.test :as test]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(deftest reducible-query-test
  (is (= [{:count 4}]
         (reduce u/default-rf [] (query/reducible-query :test/postgres "SELECT count(*) FROM people;"))))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= [{:count 4}]
             (reduce u/default-rf [] (query/reducible-query "SELECT count(*) FROM people;")))))))

(deftest query-test
  (is (= [{:count 4}]
         (query/query :test/postgres "SELECT count(*) FROM people;")))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= [{:count 4}]
             (query/query "SELECT count(*) FROM people;")))))

  (testing "HoneySQL query"
    (is (= [{:id         1
             :name       "Cam"
             :created_at #inst "2020-04-21T23:56:00.000000000-00:00"}
            {:id         2
             :name       "Sam"
             :created_at #inst "2019-01-11T23:56:00.000000000-00:00"}
            {:id         3
             :name       "Pam"
             :created_at #inst "2020-01-01T21:56:00.000000000-00:00"}
            {:id         4
             :name       "Tam"
             :created_at #inst "2020-05-25T19:56:00.000000000-00:00"}]
           (query/query :test/postgres {:select [:*], :from [:people]})))))

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

(m/defmethod query/reducible-query* [::not-even-jdbc :default]
  [connectable k options]
  (reify clojure.lang.IReduceInit
    (reduce [_ rf init]
      (reduce rf init [{k 1} {k 2} {k 3}]))))

(deftest wow-dont-even-need-to-use-jdbc-test
  (is (= [{:a 1} {:a 2} {:a 3}]
         (query/query ::not-even-jdbc :a))))
