(ns toucan2.execute-test
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]
   [toucan2.test :as test]
   [toucan2.tools.named-query :as tools.named-query])
  (:import
   (java.time LocalDateTime OffsetDateTime)))

(set! *warn-on-reflection* true)

;; TODO -- not 100% sure it makes sense for Toucan to be doing the magic key transformations automatically here without
;; us even asking!

(deftest ^:parallel reducible-query-test
  (testing "raw SQL"
    (is (= [{:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56-00:00")}
            {:id 2, :name "Sam", :created_at (OffsetDateTime/parse "2019-01-11T23:56-00:00")}
            {:id 3, :name "Pam", :created_at (OffsetDateTime/parse "2020-01-01T21:56-00:00")}
            {:id 4, :name "Tam", :created_at (OffsetDateTime/parse "2020-05-25T19:56-00:00")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (execute/reducible-query ::test/db "SELECT * FROM people ORDER BY id ASC;")))))
  (testing "SQL + params"
    (is (= [{:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (transduce
            (map (fn [row] (into {} row)))
            conj
            []
            (execute/reducible-query ::test/db ["SELECT * FROM people WHERE id = ?" 1]))))))

(deftest ^:parallel reducible-query-test-2
  (is (= [{:count 4}]
         (let [query (execute/reducible-query ::test/db "SELECT count(*) AS \"count\" FROM people;")]
           (into [] (map realize/realize) query))))

  (testing "throw an error if there is no current connection and no default connection"
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"No default Toucan connection defined"
         (into []
               (map #(select-keys % [:count]))
               (execute/reducible-query "SELECT count(*) AS \"count\" FROM people;")))))

  (testing "with current connection"
    (binding [conn/*current-connectable* ::test/db]
      (is (= [{:count 4}]
             (into []
                   (map #(select-keys % [:count]))
                   (execute/reducible-query "SELECT count(*) AS \"count\" FROM people;"))))))

  (testing "with model default connection"
    (is (= [{:count 4}]
           (into []
                 (map #(select-keys % [:count]))
                 (execute/reducible-query nil ::test/venues "SELECT count(*) AS \"count\" FROM people;")))))

  (testing "eductions"
    (is (= [{:id 2, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")}]
           (into
            []
            (map realize/realize)
            (eduction
             #_{:clj-kondo/ignore [:unused-binding]}
             (comp (map #(update % :id inc))
                   (take 1))
             (execute/reducible-query ::test/db "SELECT * FROM people;")))))))

(deftest ^:parallel query-test
  (let [expected [{:id 1, :created_at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 2, :created_at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 3, :created_at (LocalDateTime/parse "2017-01-01T00:00")}]]
    (testing "SQL"
      (is (= expected
             (execute/query ::test/db "SELECT id, created_at FROM venues ORDER BY id ASC;"))))
    (testing "HoneySQL"
      (is (= expected
             (execute/query ::test/db {:select [:id :created_at], :from [:venues], :order-by [[:id :asc]]}))))))

(deftest ^:parallel kebab-case-query-test
  (let [expected [{:id 1, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 2, :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                  {:id 3, :created-at (LocalDateTime/parse "2017-01-01T00:00")}]]
    (binding [jdbc.options/*options* {:label-fn csk/->kebab-case}]
      (testing "SQL"
        (is (= expected
               (execute/query ::test/db "SELECT id, created_at FROM venues ORDER BY id ASC;"))))
      (testing "HoneySQL"
        (is (= expected
               (execute/query ::test/db {:select [:id :created-at], :from [:venues], :order-by [[:id :asc]]})))))))

(tools.named-query/define-named-query ::named-query
  ["SELECT count(*) AS \"count\" FROM people;"])

(deftest ^:parallel query-test-2
  (is (= [{:count 4}]
         (execute/query ::test/db "SELECT count(*) AS \"count\" FROM people;")))
  (testing "with current connection"
    (binding [conn/*current-connectable* ::test/db]
      (is (= [{:count 4}]
             (execute/query "SELECT count(*) AS \"count\" FROM people;")))))
  (testing "HoneySQL query"
    (is (= [{:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (OffsetDateTime/parse "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (OffsetDateTime/parse "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (OffsetDateTime/parse "2020-05-25T19:56Z")}]
           (execute/query ::test/db {:select [:*], :from [:people]})))))

(deftest ^:parallel execute-named-query-test
  (testing "named query"
    (is (= [{:count 4}]
           (execute/query ::test/db ::named-query)))))

(deftest ^:parallel query-one-test
  (is (= {:count 4}
         (execute/query-one ::test/db "SELECT count(*) AS \"count\" FROM people")))

  (testing "with current connection"
    (binding [conn/*current-connectable* ::test/db]
      (is (= {:count 4}
             (execute/query-one "SELECT count(*) AS \"count\" FROM people;"))))))

(deftest ^:synchronized query-update-count-test
  (testing "For query types returning an update count, both `query` and `query-one` should return the count"
    (doseq [f [#'execute/query
               #'execute/query-one]]
      (testing f
        (test/with-discarded-table-changes :people
          (is (= 1
                 (f
                  ::test/db
                  :toucan.result-type/update-count
                  nil
                  ["UPDATE people SET name = ? WHERE name = ?;"
                   "TouCam"
                   "Cam"])))))))
  (testing "default query type"
    (doseq [[f expected] {#'execute/query     [1]
                          #'execute/query-one 1}]
      (testing f
        (test/with-discarded-table-changes :people
          (is (= expected
                 (f
                  ::test/db
                  nil
                  ["UPDATE people SET name = ? WHERE name = ?;"
                   "TouCam"
                   "Cam"]))))))))

(deftest ^:parallel reducible-query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")})]
         (realize/realize (execute/reducible-query ::test/db :people "SELECT * FROM people WHERE id = 1;")))))

(deftest ^:parallel kebab-case-reducible-query-as-test
  (binding [jdbc.options/*options* {:label-fn csk/->kebab-case}]
    (is (= [(instance/instance :people {:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})]
           (realize/realize (execute/reducible-query ::test/db :people "SELECT * FROM people WHERE id = 1;"))))))

(deftest ^:parallel query-as-test
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
(m/defmethod pipeline/transduce-execute-with-connection [#_connection ::connectable.not-even-jdbc
                                                         #_query-type :default
                                                         #_model      :default]
  [rf _conn _query-type _model [{k :key}, :as _compiled-query]]
  (transduce identity rf [{k 1} {k 2} {k 3}]))

(deftest ^:parallel wow-dont-even-need-to-use-jdbc-test
  (is (= [{:a 1} {:a 2} {:a 3}]
         (execute/query ::connectable.not-even-jdbc [{:key :a}]))))

(m/defmethod pipeline/transduce-query [:default ::model.not-even-jdbc :default]
  [rf query-type model _parsed-args resolved-query]
  (#'pipeline/transduce-execute rf query-type model resolved-query))

;;; here's how you can have custom compilation behavior. At this point in time it requires specifying a model as well
;;; since connection isn't realized until after the query compilation stage.

(m/defmethod pipeline/transduce-execute-with-connection [#_connection ::connectable.not-even-jdbc
                                                         #_query-type :default
                                                         #_model      ::model.not-even-jdbc]
  [rf _conn _query-type _model {k :key, :as _compiled-query}]
  (transduce identity rf [{k 4} {k 5} {k 6}]))

(deftest ^:parallel wow-dont-even-need-to-use-jdbc-custom-model-test
  (is (= [{:a 4} {:a 5} {:a 6}]
         (execute/query ::connectable.not-even-jdbc ::model.not-even-jdbc {:key :a}))))

(deftest ^:synchronized execute!-test
  (try
    (is (= 0
           (execute/query-one ::test/db "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
    (is (= []
           (execute/query ::test/db "SELECT * FROM execute_test_table;")))
    (finally
      (execute/query ::test/db "DROP TABLE IF EXISTS execute_test_table;"))))

(deftest ^:parallel with-call-counts-test
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

(deftest ^:parallel current-connectable-test
  (binding [conn/*current-connectable* ::test/db]
    (is (= [{:one 1}]
           (execute/query "SELECT 1 AS one;")))))

(deftest ^:synchronized default-connectable-for-model-test
  (testing "Should be able to query things using the default connectables for a model"
    (is (= [{:one 1}]
           (execute/query nil ::test/venues "SELECT 1 AS one;")))
    (test/with-discarded-table-changes :venues
      (is (= [1]
             (execute/query nil ::test/venues "DELETE FROM venues WHERE id = 1;"))))))

(deftest ^:parallel explicit-connectable-should-override-default-test
  (binding [conn/*current-connectable* :fake-db]
    (is (= [{:one 1}]
           (execute/query ::test/db "SELECT 1 AS one;")))))

(deftest ^:parallel query-returning-no-columns-test
  ;; this syntax doesn't seem to work on MariaDB
  (when (#{:h2 :postgres} (test/current-db-type))
    (is (= [(instance/instance nil {})
            (instance/instance nil {})
            (instance/instance nil {})]
           (execute/query ::test/db ["SELECT FROM venues;"])))))
