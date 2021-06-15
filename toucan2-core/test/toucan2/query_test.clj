(ns toucan2.query-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]
            [next.jdbc.result-set :as next.jdbc.rs]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.instance :as instance]
            toucan2.jdbc
            [toucan2.query :as query]
            [toucan2.queryable :as queryable]
            [toucan2.realize :as realize]
            [toucan2.test :as test]))

(comment toucan2.jdbc/keep-me)

(use-fixtures :once test/do-with-test-data)

(deftest reducible-query-test
  (is (= [{:count 4}]
         (let [query (query/reducible-query :test/postgres "SELECT count(*) FROM people;")]
           (into [] (map realize/realize) query))))

  (testing "with current connection"
    (conn/with-connection :test/postgres
      (is (= [{:count 4}]
             (into [] (map #(select-keys % [:count])) (query/reducible-query "SELECT count(*) FROM people;"))))))

  (testing "eductions"
    (is (= [{:id 2, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56Z")}]
           (into
            []
            (map realize/realize)
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
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created-at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created-at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created-at (t/offset-date-time "2020-05-25T19:56Z")}]
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

(deftest execute!-test
  (try
    ;; TODO -- should `update!` just return `0` instead of the `:update-count` key?
    (is (= 0
           (query/execute! :test/postgres "CREATE TABLE \"execute_test_table\" (\"id\" INTEGER NOT NULL);")))
    (is (= []
           (query/query :test/postgres "SELECT * FROM \"execute_test_table\";")))
    (finally
      (query/execute! :test/postgres "DROP TABLE IF EXISTS \"execute_test_table\";"))))

(deftest with-call-counts-test
  (query/with-call-count [call-count]
    (is (= 0
           (call-count)))
    (query/query :test/postgres "SELECT 1;")
    (is (= 1
           (call-count)))
    (query/execute! :test/postgres "DELETE FROM people WHERE id = 1000;")
    (is (= 2
           (call-count)))
    (testing "Should be able to do nested calls to with-call-count"
      (query/with-call-count [nested-call-count]
        (is (= 2
               (call-count)))
        (is (= 0
               (nested-call-count)))
        (query/query :test/postgres "SELECT 1;")
        (is (= 3
               (call-count)))
        (is (= 1
               (nested-call-count)))))
    (is (= 3
           (call-count)))))

(m/defmethod conn.current/default-connectable-for-tableable* ::venues
  [_ _]
  :test/postgres)

(deftest default-connectable-for-tableable-test
  (is (= [{:one 1}]
         (query/query nil ::venues "SELECT 1 AS one;")))
  (test/with-venues-reset
    (is (= 1
           (query/execute! nil ::venues "DELETE FROM venues WHERE id = 1;")))))

(deftest readable-column-test
  (testing "Toucan 2 should call next.jdbc.result-set/read-column-by-index"
    (is (= [{:n 100.0M}]
           (query/query :test/postgres "SELECT '100.0'::decimal AS n;")))
    (try
      (extend-protocol next.jdbc.rs/ReadableColumn
        java.math.BigDecimal
        (read-column-by-index [n _ _]
          (str n)))
      (is (= [{:n "100.0"}]
             (query/query :test/postgres "SELECT '100.0'::decimal AS n;")))
      (finally
        ;; reverse the changes.
        (extend-protocol next.jdbc.rs/ReadableColumn
          java.math.BigDecimal
          (read-column-by-index [n _ _]
            n))))))

(deftest execute-reducible-test
  (testing "execute! should return a reducible query if you pass `:reducible?` in the options"
    (let [query (query/execute! :test/postgres nil "SELECT 1 AS one;" {:reducible? true})]
      (is (instance? toucan2.jdbc.query.ReducibleQuery query))
      (is (= [(instance/instance :test/postgres nil {:one 1})]
             (realize/realize query))))))
