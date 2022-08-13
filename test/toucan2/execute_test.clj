(ns toucan2.execute-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
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
            (execute/reducible-query ::test/db "SELECT * FROM people ORDER BY id ASC;")))))
  (testing "SQL + params"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (execute/reducible-query ::test/db ["SELECT * FROM people WHERE id = ?" 1]))))))

(deftest reducible-query-test-2
  (is (= [{:count 4}]
         (let [query (execute/reducible-query ::test/db "SELECT count(*) FROM people;")]
           (into [] (map realize/realize) query))))

  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= [{:count 4}]
             (into [] (map #(select-keys % [:count])) (execute/reducible-query "SELECT count(*) FROM people;"))))))

  (testing "eductions"
    (is (= [{:id 2, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (into
            []
            (map realize/realize)
            (eduction
             (comp (map #(update % :id inc))
                   (take 1))
             (execute/reducible-query ::test/db "SELECT * FROM people;")))))))

(deftest query-test
  (let [expected [{:id 1, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 2, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 3, :created-at (LocalDateTime/parse "2017-01-01T00:00")}]]
    (testing "SQL"
      (execute/query ::test/db "SELECT * FROM venues ORDER BY id ASC;"))
    (testing "HoneySQL"
      (is (= expected
             (execute/query ::test/db {:select [:id :created_at], :from [:venues], :order-by [[:id :asc]]}))))))

(m/defmethod compile/do-with-compiled-query [:default ::named-query]
  [_model _query f]
  (f ["SELECT count(*) FROM people;"]))

(deftest query-test-2
  (is (= [{:count 4}]
         (execute/query ::test/db "SELECT count(*) FROM people;")))
  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= [{:count 4}]
             (execute/query "SELECT count(*) FROM people;")))))
  (testing "HoneySQL query"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created-at (OffsetDateTime/parse "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created-at (OffsetDateTime/parse "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created-at (OffsetDateTime/parse "2020-05-25T19:56Z")}]
           (execute/query ::test/db {:select [:*], :from [:people]}))))
  (testing "named query"
    (is (= [{:count 4}]
           (execute/query ::test/db ::named-query)))))

(deftest query-one-test
  (is (= {:count 4}
         (execute/query-one ::test/db "SELECT count(*) FROM people")))

  (testing "with current connection"
    (binding [current/*connection* ::test/db]
      (is (= {:count 4}
             (execute/query-one "SELECT count(*) FROM people;"))))))

(deftest reducible-query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})]
         (realize/realize (execute/reducible-query ::test/db :people "SELECT * FROM people WHERE id = 1;")))))

(deftest query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam"})
          (instance/instance :people {:id 2, :name "Sam"})
          (instance/instance :people {:id 3, :name "Pam"})
          (instance/instance :people {:id 4, :name "Tam"})]
         (execute/query ::test/db :people {:select [:id :name], :from [:people]}))))

(m/defmethod conn/do-with-connection ::connectable.identity
  [connectable f]
  (f connectable))

(derive ::connectable.not-even-jdbc ::connectable.identity)

;;; TODO -- not super happy we can't just use a plain map or arbitrary query here, because it will try to get compiled
;;; as HoneySQL. There is currently no way to define custom compilation behavior on the basis of the connectable. Not
;;; sure how this would actually work tho without realizing the connection *first*; that causes its own problems because
;;; it breaks [[toucan2.tools.identity-execute/identity-query]]

(m/defmethod execute/reduce-compiled-query-with-connection [::connectable.not-even-jdbc :default clojure.lang.Sequential]
  [_conn _model [{k :key}] rf init]
  (reduce rf init [{k 1} {k 2} {k 3}]))

(deftest wow-dont-even-need-to-use-jdbc-test
  (is (= [{:a 1} {:a 2} {:a 3}]
         (execute/query ::connectable.not-even-jdbc [{:key :a}]))))

(m/defmethod execute/reduce-uncompiled-query [::model.not-even-jdbc :default]
  [connectable model query rf init]
  (execute/reduce-compiled-query connectable model query rf init))

;;; here's how you can have custom compilation behavior. At this point in time it requires specifying a model as well
;;; since connection isn't realized until after the query compilation stage.

(m/defmethod execute/reduce-compiled-query-with-connection [::connectable.not-even-jdbc ::model.not-even-jdbc clojure.lang.IPersistentMap]
  [_conn _model {k :key} rf init]
  (reduce rf init [{k 4} {k 5} {k 6}]))

(deftest wow-dont-even-need-to-use-jdbc-custom-model-test
  (is (= [{:a 4} {:a 5} {:a 6}]
         (execute/query ::connectable.not-even-jdbc ::model.not-even-jdbc {:key :a})
         ;; TODO -- should be able to test `select` with this as well
         )))

(deftest execute!-test
  (try
    (is (= 0
           (execute/query-one ::test/db "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
    (is (= []
           (execute/query ::test/db "SELECT * FROM execute_test_table;")))
    (finally
      (execute/query ::test/db "DROP TABLE IF EXISTS execute_test_table;"))))

(deftest compile-test
  (is (= ["SELECT * FROM people WHERE id > ?" 1]
         (execute/compile (select/select ::test/people :id [:> 1])))))

;;; TODO

#_(deftest with-call-counts-test
    (execute/with-call-count [call-count]
      (is (= 0
             (call-count)))
      (execute/query ::test/db "SELECT 1;")
      (is (= 1
             (call-count)))
      (execute/query ::test/db "DELETE FROM people WHERE id = 1000;")
      (is (= 2
             (call-count)))
      (testing "Should be able to do nested calls to with-call-count"
        (execute/with-call-count [nested-call-count]
          (is (= 2
                 (call-count)))
          (is (= 0
                 (nested-call-count)))
          (execute/query ::test/db "SELECT 1;")
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
         (execute/query nil ::venues "SELECT 1 AS one;")))
  (test/with-venues-reset
    (is (= 1
           (execute/query nil ::venues "DELETE FROM venues WHERE id = 1;")))))

#_(deftest readable-column-test
  (testing "Toucan 2 should call next.jdbc.result-set/read-column-by-index"
    (is (= [{:n 100.0M}]
           (execute/query ::test/db "SELECT '100.0'::decimal AS n;")))
    (try
      (extend-protocol next.jdbc.rs/ReadableColumn
        java.math.BigDecimal
        (read-column-by-index [n _ _]
          (str n)))
      (is (= [{:n "100.0"}]
             (execute/query ::test/db "SELECT '100.0'::decimal AS n;")))
      (finally
        ;; reverse the changes.
        (extend-protocol next.jdbc.rs/ReadableColumn
          java.math.BigDecimal
          (read-column-by-index [n _ _]
            n))))))
