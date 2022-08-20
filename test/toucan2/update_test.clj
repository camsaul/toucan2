(ns toucan2.update-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(use-fixtures :each test/do-db-types-fixture)

(deftest parse-update-args-test
  (is (= {:changes {:a 1}, :kv-args {:toucan/pk 1}, :queryable {}}
         (query/parse-args ::update/update nil [1 {:a 1}])))
  (is (= {:changes {:a 1}, :kv-args {:toucan/pk nil}, :queryable {}}
         (query/parse-args ::update/update nil [nil {:a 1}])))
  (is (= {:kv-args {:id 1}, :changes {:a 1}, :queryable {}}
         (query/parse-args ::update/update nil [:id 1 {:a 1}])))
  (testing "composite PK"
    (is (= {:changes {:a 1}, :kv-args {:toucan/pk [1 2]}, :queryable {}}
           (query/parse-args ::update/update nil [[1 2] {:a 1}]))))
  (testing "key-value conditions"
    (is (= {:kv-args {:name "Cam", :toucan/pk 1}, :changes {:a 1}, :queryable {}}
           (query/parse-args ::update/update nil [1 :name "Cam" {:a 1}]))))
  (is (= {:changes {:name "Hi-Dive"}, :queryable {:id 1}}
         (query/parse-args ::update/update nil [{:id 1} {:name "Hi-Dive"}]))))

(deftest build-test
  (is (= {:update [:venues]
          :set    {:name "Hi-Dive"}
          :where  [:and
                   [:= :name "Tempest"]
                   [:= :id 1]]}
         (query/build ::update/update ::test/venues {:changes {:name "Hi-Dive"}
                                                     :query   {:id 1}
                                                     :kv-args {:name "Tempest"}}))))

(deftest pk-and-map-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues 1 {:name "Hi-Dive"})))
    (is (= (instance/instance ::test/venues {:id         1
                                             :name       "Hi-Dive"
                                             :category   "bar"
                                             :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                             :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           (select/select-one ::test/venues 1)))))

(deftest key-value-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues :name "Tempest" {:name "Hi-Dive"})))))

(derive ::venues.composite-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.composite-pk
  [_model]
  [:id :name])

(deftest composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::venues.composite-pk [1 "Tempest"] {:name "Hi-Dive"})))))

;; (m/defmethod honeysql.compile/to-sql* [:default ::venues.custom-honeysql :id String]
;;   [_ _ _ v _]
;;   (assert (string? v) (format "V should be a string, got %s" (pr-str v)))
;;   ["?::integer" v])

;; (derive ::venues.custom-honeysql-composite-pk ::venues.composite-pk)
;; (derive ::venues.custom-honeysql-composite-pk ::venues.custom-honeysql)

;; (deftest update!-custom-honeysql-test
;;   (testing "custom HoneySQL for PK"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql "1" {:name "Hi-Dive"})))
;;       (is (= (instance/instance ::venues.custom-honeysql {:id 1, :name "Hi-Dive"})
;;              (select/select-one ::venues.custom-honeysql "1" {:select [:id :name]}))))
;;     (testing "composite PK"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (update/update! ::venues.custom-honeysql-composite-pk ["1" "Tempest"] {:name "Hi-Dive"}))))))
;;   (testing "custom HoneySQL for key-value conditions"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql :id "1" {:name "Hi-Dive"})))))
;;   (testing "custom HoneySQL for changes"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql :id "1" {:id "100"})))
;;       (is (= (instance/instance ::venues.custom-honeysql {:id 100, :name "Tempest"})
;;              (select/select-one ::venues.custom-honeysql "100" {:select [:id :name]}))))))

(deftest update!-no-changes-no-op-test
  (testing "If there are no changes, update! should no-op and return zero"
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues 1 {})
             (update/update! ::test/venues {}))))))

(m/defmethod query/do-with-query [:default ::named-conditions]
  [model _queryable f]
  (query/do-with-query model {:id 1} f))

(deftest named-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= "Tempest"
           (select/select-one-fn :name ::test/venues 1)))
    (is (= 1
           (update/update! ::test/venues ::named-conditions {:name "Grant & Green"})))
    (is (= "Grant & Green"
           (select/select-one-fn :name ::test/venues 1)))))

(deftest update-returning-pks-test
  (test/with-discarded-table-changes :venues
    (is (= [1 2]
           ;; the order these come back in is indeterminate but as long as we get back a sequence of [1 2] we're fine
           (sort (update/update-returning-pks! ::test/venues :category "bar" {:category "BARRR"}))))
    (is (= [(instance/instance ::test/venues {:id 1, :name "Tempest", :category "BARRR"})
            (instance/instance ::test/venues {:id 2, :name "Ho's Tavern", :category "BARRR"})]
           (select/select [::test/venues :id :name :category] :category "BARRR" {:order-by [[:id :asc]]})))))

(deftest update-nil-test
  (testing "(update! model nil ...) should basically be the same as (update! model :toucan/pk nil ...)"
    (is (= {:kv-args {:toucan/pk nil}, :changes {:name "Taco Bell"}, :queryable {}}
           (query/parse-args ::update/update nil [nil {:name "Taco Bell"}])))
    (query/with-parsed-args-with-query [parsed-args [::update/update ::test/venues [nil {:name "Taco Bell"}]]]
      (is (= {:kv-args {:toucan/pk nil}, :changes {:name "Taco Bell"}, :query {}}
             parsed-args))
      (is (= {:update [:venues]
              :set    {:name "Taco Bell"}
              :where  [:= :id nil]}
             (query/build ::update/update ::test/venues parsed-args))))
    (is (= ["UPDATE venues SET name = ? WHERE id IS NULL" "Taco Bell"]
           (execute/compile
             (update/update! ::test/venues nil {:name "Taco Bell"}))))
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues nil {:name "Taco Bell"}))))))
