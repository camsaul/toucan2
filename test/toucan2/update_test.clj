(ns toucan2.update-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(deftest parse-update-args-test
  (is (= {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk 1}, :queryable {}}
         (query/parse-args :toucan.query-type/update.* [:model 1 {:a 1}])))
  (is (= {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk nil}, :queryable {}}
         (query/parse-args :toucan.query-type/update.* [:model nil {:a 1}])))
  (is (= {:modelable :model, :kv-args {:id 1}, :changes {:a 1}, :queryable {}}
         (query/parse-args :toucan.query-type/update.* [:model :id 1 {:a 1}])))
  (testing "composite PK"
    (is (= {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk [1 2]}, :queryable {}}
           (query/parse-args :toucan.query-type/update.* [:model [1 2] {:a 1}]))))
  (testing "key-value conditions"
    (is (= {:modelable :model, :kv-args {:name "Cam", :toucan/pk 1}, :changes {:a 1}, :queryable {}}
           (query/parse-args :toucan.query-type/update.* [:model 1 :name "Cam" {:a 1}]))))
  (is (= {:modelable :model, :changes {:name "Hi-Dive"}, :queryable {:id 1}}
         (query/parse-args :toucan.query-type/update.* [:model {:id 1} {:name "Hi-Dive"}]))))

(deftest build-test
  (is (= {:update [:venues]
          :set    {:name "Hi-Dive"}
          :where  [:and
                   [:= :name "Tempest"]
                   [:= :id 1]]}
         (query/build :toucan.query-type/update.*
                      ::test/venues
                      {:changes {:name "Hi-Dive"}
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

(deftest update!-no-changes-no-op-test
  (testing "If there are no changes, update! should no-op and return zero"
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues 1 {})
             (update/update! ::test/venues {}))))))

(m/defmethod query/do-with-resolved-query [:default ::named-conditions]
  [model _queryable f]
  (query/do-with-resolved-query model {:id 1} f))

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
    (let [parsed-args (query/parse-args :toucan.query-type/update.* [::test/venues nil {:name "Taco Bell"}])]
      (is (= {:modelable ::test/venues
              :kv-args   {:toucan/pk nil}
              :changes   {:name "Taco Bell"}
              :queryable {}}
             parsed-args))
      (query/with-resolved-query [query [::test/venues (:queryable parsed-args)]]
        (is (= {}
               query))
        (is (= {:update    [:venues]
                :set       {:name "Taco Bell"}
                :where     [:= :id nil]}
               (query/build :toucan.query-type/update.* ::test/venues (assoc parsed-args :query query))))))
    (is (= ["UPDATE venues SET name = ? WHERE id IS NULL" "Taco Bell"]
           (tools.compile/compile
             (update/update! ::test/venues nil {:name "Taco Bell"}))))
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues nil {:name "Taco Bell"}))))))
