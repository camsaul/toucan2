(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.test :as test])
  (:import
   (java.time LocalDateTime OffsetDateTime)))

;; TODO -- not 100% sure it makes sense for Toucan to be doing the magic key transformations automatically here without
;; us even asking!

(deftest reducible-query-test
  (testing "raw SQL"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56-00:00")}
            {:id 2, :name "Sam", :created-at (OffsetDateTime/parse "2019-01-11T23:56-00:00")}
            {:id 3, :name "Pam", :created-at (OffsetDateTime/parse "2020-01-01T21:56-00:00")}
            {:id 4, :name "Tam", :created-at (OffsetDateTime/parse "2020-05-25T19:56-00:00")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db "SELECT * FROM people ORDER BY id ASC;")))))
  (testing "SQL + params"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (query/reducible-query ::test/db ["SELECT * FROM people WHERE id = ?" 1]))))))

(deftest reducible-query-test-2
  (is (= [{:count 4}]
         (let [query (query/reducible-query ::test/db "SELECT count(*) FROM people;")]
           (into [] (map realize/realize) query))))

  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= [{:count 4}]
             (into [] (map #(select-keys % [:count])) (query/reducible-query "SELECT count(*) FROM people;"))))))

  (testing "eductions"
    (is (= [{:id 2, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (into
            []
            (map realize/realize)
            (eduction
             (comp (map #(update % :id inc))
                   (take 1))
             (query/reducible-query ::test/db "SELECT * FROM people;")))))))

(deftest query-test
  (let [expected [{:id 1, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 2, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 3, :created-at (LocalDateTime/parse "2017-01-01T00:00")}]]
    (testing "SQL"
      (query/query ::test/db "SELECT * FROM venues ORDER BY id ASC;"))
    (testing "HoneySQL"
      (is (= expected
             (query/query ::test/db {:select [:id :created_at], :from [:venues], :order-by [[:id :asc]]}))))))

(m/defmethod compile/do-with-compiled-query [:default ::named-query]
  [_conn _query f]
  (f ["SELECT count(*) FROM people;"])
  #_(compile/do-with-compiled-query conn ))

(deftest query-test-2
  (is (= [{:count 4}]
         (query/query ::test/db "SELECT count(*) FROM people;")))
  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= [{:count 4}]
             (query/query "SELECT count(*) FROM people;")))))
  (testing "HoneySQL query"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created-at (OffsetDateTime/parse "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created-at (OffsetDateTime/parse "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created-at (OffsetDateTime/parse "2020-05-25T19:56Z")}]
           (query/query ::test/db {:select [:*], :from [:people]}))))
  (testing "named query"
    (is (= [{:count 4}]
           (query/query ::test/db ::named-query)))))

(deftest query-one-test
  (is (= {:count 4}
         (query/query-one ::test/db "SELECT count(*) FROM people")))

  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= {:count 4}
             (query/query-one "SELECT count(*) FROM people;"))))))

(deftest reducible-query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")})]
         (realize/realize (query/reducible-query-as ::test/db :people "SELECT * FROM people WHERE id = 1;")))))

(deftest query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam"})
          (instance/instance :people {:id 2, :name "Sam"})
          (instance/instance :people {:id 3, :name "Pam"})
          (instance/instance :people {:id 4, :name "Tam"})]
         (query/query-as ::test/db :people {:select [:id :name], :from [:people]}))))

(m/defmethod conn/do-with-connection ::not-even-jdbc
  [connectable f]
  (f connectable))

(m/defmethod compile/do-with-compiled-query [::not-even-jdbc :default]
  [_conn query f]
  (f query))

(m/defmethod query/reduce-query [::not-even-jdbc :default]
  [_connectable k rf init]
  (reduce rf init [{k 1} {k 2} {k 3}]))

(deftest wow-dont-even-need-to-use-jdbc-test
  (is (= [{:a 1} {:a 2} {:a 3}]
         (query/query ::not-even-jdbc :a))))

(deftest execute!-test
  (try
    ;; TODO -- should this just return the number of rows affected?
    (is (= [{:next.jdbc/update-count 0}]
           (query/execute! ::test/db "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
    (is (= []
           (query/query ::test/db "SELECT * FROM execute_test_table;")))
    (finally
      (query/execute! ::test/db "DROP TABLE IF EXISTS execute_test_table;"))))

;; TODO

#_(deftest with-call-counts-test
    (query/with-call-count [call-count]
      (is (= 0
             (call-count)))
      (query/query ::test/db "SELECT 1;")
      (is (= 1
             (call-count)))
      (query/execute! ::test/db "DELETE FROM people WHERE id = 1000;")
      (is (= 2
             (call-count)))
      (testing "Should be able to do nested calls to with-call-count"
        (query/with-call-count [nested-call-count]
          (is (= 2
                 (call-count)))
          (is (= 0
                 (nested-call-count)))
          (query/query ::test/db "SELECT 1;")
          (is (= 3
                 (call-count)))
          (is (= 1
                 (nested-call-count)))))
      (is (= 3
             (call-count)))))

#_(m/defmethod conn.current/default-connectable-for-tableable* ::venues
    [_ _]
    ::test/db)

#_(deftest default-connectable-for-tableable-test
  (is (= [{:one 1}]
         (query/query nil ::venues "SELECT 1 AS one;")))
  (test/with-venues-reset
    (is (= 1
           (query/execute! nil ::venues "DELETE FROM venues WHERE id = 1;")))))

#_(deftest readable-column-test
  (testing "Toucan 2 should call next.jdbc.result-set/read-column-by-index"
    (is (= [{:n 100.0M}]
           (query/query ::test/db "SELECT '100.0'::decimal AS n;")))
    (try
      (extend-protocol next.jdbc.rs/ReadableColumn
        java.math.BigDecimal
        (read-column-by-index [n _ _]
          (str n)))
      (is (= [{:n "100.0"}]
             (query/query ::test/db "SELECT '100.0'::decimal AS n;")))
      (finally
        ;; reverse the changes.
        (extend-protocol next.jdbc.rs/ReadableColumn
          java.math.BigDecimal
          (read-column-by-index [n _ _]
            n))))))

#_(deftest execute-reducible-test
  (testing "execute! should return a reducible query if you pass `:reducible?` in the options"
    (let [query (query/execute! ::test/db nil "SELECT 1 AS one;" {:reducible? true})]
      (is (instance? toucan2.jdbc.query.ReducibleQuery query))
      (is (= [(instance/instance ::test/db nil {:one 1})]
             (realize/realize query))))))
