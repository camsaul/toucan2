(ns toucan2.update-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.named-query :as tools.named-query]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(deftest ^:parallel parse-update-args-test
  (are [args expected] (= expected
                          (update/parse-update-args :toucan.query-type/update.* args))

    [:model 1 {:a 1}]                            {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk 1}, :queryable {}}
    [:model nil {:a 1}]                          {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk nil}, :queryable {}}
    [:model :id 1 {:a 1}]                        {:modelable :model, :kv-args {:id 1}, :changes {:a 1}, :queryable {}}
    [:model [1 2] {:a 1}]                        {:modelable :model, :changes {:a 1}, :kv-args {:toucan/pk [1 2]}, :queryable {}}
    [:model 1 :name "Cam" {:a 1}]                {:modelable :model, :kv-args {:name "Cam", :toucan/pk 1}, :changes {:a 1}, :queryable {}}
    [:model {:id 1} {:name "Hi-Dive"}]           {:modelable :model, :changes {:name "Hi-Dive"}, :queryable {:id 1}}
    [:conn :db :model 1 {:a 1}]                  {:connectable :db, :modelable :model, :changes {:a 1}, :kv-args {:toucan/pk 1}, :queryable {}}
    [:conn :db :model nil {:a 1}]                {:connectable :db, :modelable :model, :changes {:a 1}, :kv-args {:toucan/pk nil}, :queryable {}}
    [:conn :db :model :id 1 {:a 1}]              {:connectable :db, :modelable :model, :kv-args {:id 1}, :changes {:a 1}, :queryable {}}
    [:conn :db :model [1 2] {:a 1}]              {:connectable :db, :modelable :model, :changes {:a 1}, :kv-args {:toucan/pk [1 2]}, :queryable {}}
    [:conn :db :model 1 :name "Cam" {:a 1}]      {:connectable :db, :modelable :model, :kv-args {:name "Cam", :toucan/pk 1}, :changes {:a 1}, :queryable {}}
    [:conn :db :model {:id 1} {:name "Hi-Dive"}] {:connectable :db, :modelable :model, :changes {:name "Hi-Dive"}, :queryable {:id 1}}))

(deftest ^:parallel build-test
  (is (= {:update [:venues]
          :set    {:name "Hi-Dive"}
          :where  [:and
                   [:= :name "Tempest"]
                   [:= :id 1]]}
         (pipeline/build :toucan.query-type/update.*
                         ::test/venues
                         {:changes {:name "Hi-Dive"}
                          :kv-args {:name "Tempest"}}
                         {:id 1}))))

(deftest ^:synchronized pk-and-map-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues 1 {:name "Hi-Dive"})))
    (is (= (instance/instance ::test/venues {:id         1
                                             :name       "Hi-Dive"
                                             :category   "bar"
                                             :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                             :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           (select/select-one ::test/venues 1)))))

(deftest ^:synchronized key-value-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues :name "Tempest" {:name "Hi-Dive"})))))

(derive ::venues.composite-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.composite-pk
  [_model]
  [:id :name])

(deftest ^:synchronized composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::venues.composite-pk [1 "Tempest"] {:name "Hi-Dive"})))))

(deftest ^:synchronized update!-no-changes-no-op-test
  (testing "If there are no changes, update! should no-op and return zero"
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues 1 {})
             (update/update! ::test/venues {}))))))

(tools.named-query/define-named-query ::named-conditions
  {:id 1})

(deftest ^:synchronized named-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= "Tempest"
           (select/select-one-fn :name ::test/venues 1)))
    (is (= 1
           (update/update! ::test/venues ::named-conditions {:name "Grant & Green"})))
    (is (= "Grant & Green"
           (select/select-one-fn :name ::test/venues 1)))))

(deftest ^:synchronized update-returning-pks-test
  (test/with-discarded-table-changes :venues
    (is (= [1 2]
           ;; the order these come back in is indeterminate but as long as we get back a sequence of [1 2] we're fine
           (sort (update/update-returning-pks! ::test/venues :category "bar" {:category "BARRR"}))))
    (is (= [(instance/instance ::test/venues {:id 1, :name "Tempest", :category "BARRR"})
            (instance/instance ::test/venues {:id 2, :name "Ho's Tavern", :category "BARRR"})]
           (select/select [::test/venues :id :name :category] :category "BARRR" {:order-by [[:id :asc]]})))))

(deftest ^:synchronized update-nil-test
  (testing "(update! model nil ...) should basically be the same as (update! model :toucan/pk nil ...)"
    (let [parsed-args (update/parse-update-args :toucan.query-type/update.* [::test/venues nil {:name "Taco Bell"}])]
      (is (= {:modelable ::test/venues
              :kv-args   {:toucan/pk nil}
              :changes   {:name "Taco Bell"}
              :queryable {}}
             parsed-args))
      (let [query (pipeline/resolve-query :toucan.query-type/update.* ::test/venues (:queryable parsed-args))]
        (is (= {}
               query))
        (is (= {:update [:venues]
                :set    {:name "Taco Bell"}
                :where  [:= :id nil]}
               (pipeline/build :toucan.query-type/update.* ::test/venues parsed-args query)))))
    (is (= [(case (test/current-db-type)
              :h2       "UPDATE \"VENUES\" SET \"NAME\" = ? WHERE \"ID\" IS NULL"
              :postgres "UPDATE \"venues\" SET \"name\" = ? WHERE \"id\" IS NULL") "Taco Bell"]
           (tools.compile/compile
             (update/update! ::test/venues nil {:name "Taco Bell"}))))
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues nil {:name "Taco Bell"}))))))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::test/venues :venue})

(deftest ^:synchronized namespaced-test
  (doseq [update! [#'update/update!
                   #'update/update-returning-pks!]]
    (test/with-discarded-table-changes :venues
      (testing update!
        (is (= (condp = update!
                 #'update/update!               1
                 #'update/update-returning-pks! [3])
               (update! ::venues.namespaced 3 {:venue/name "Grant & Green", :venue/category "bar"})))
        (is (= (instance/instance
                ::test/venues
                {:id       3
                 :name     "Grant & Green"
                 :category "bar"})
               (select/select-one [::test/venues :id :name :category] :id 3)))))))

(deftest ^:synchronized positional-connectable-test
  (testing "Support :conn positional connectable arg"
    (test/with-discarded-table-changes :venues
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No default Toucan connection defined"
           (update/update! :venues 1 {:name "Hi-Dive"})))
      (is (= 1
             (update/update! :conn ::test/db :venues 1 {:name "Hi-Dive"})))
      (testing "nil :conn should not override current connectable"
        (binding [conn/*current-connectable* ::test/db]
          (is (= 1
                 (update/update! :conn nil :venues 1 {:name "Hi-Dive"})))))
      (testing "Explicit connectable should override current connectable"
        (binding [conn/*current-connectable* :fake-db]
          (is (= 1
                 (update/update! :conn ::test/db :venues 1 {:name "Hi-Dive"})))))
      (testing "Explicit connectable should override model default connectable"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Don't know how to get a connection from .* :fake-db"
             (update/update! :conn :fake-db ::test/venues 1 {:name "Hi-Dive"})))))))
