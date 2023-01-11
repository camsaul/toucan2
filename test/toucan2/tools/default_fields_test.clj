(ns toucan2.tools.default-fields-test
  (:require
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.default-fields :as default-fields]))

(set! *warn-on-reflection* true)

(derive ::venues.default-fields ::test.track-realized/venues)

(default-fields/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest ^:parallel select-test
  (testing "In Toucan 2, default fields SHOULD NOT rewrite the query."
    (is (= {:select [:*], :from [[:venues]]}
           (pipeline/build :toucan.query-type/select.instances ::venues.default-fields {} {}))))
  (test.track-realized/with-realized-columns [realized-columns]
    (is (= [(instance/instance ::venues.default-fields
                               {:id 1, :name "Tempest", :category "bar"})
            (instance/instance ::venues.default-fields
                               {:id 2, :name "Ho's Tavern", :category "bar"})
            (instance/instance ::venues.default-fields
                               {:id 3, :name "BevMo", :category "store"})]
           (select/select ::venues.default-fields)))
    (is (= #{:venues/name :venues/id :venues/category}
           (realized-columns))
        "Only realize the specific columns we've asked for.")))

(deftest ^:parallel preserve-model-test
  (testing "Should preserve the model"
    (let [instance (select/select-one ::venues.default-fields 1)]
      (is (= {:id 1, :name "Tempest", :category "bar"}
             instance))
      (is (instance/instance? instance))
      (is (instance/instance-of? ::venues.default-fields instance))
      (is (= ::venues.default-fields
             (protocols/model instance)))
      (is (= nil
             (protocols/changes instance)))
      (is (= (into {} instance)
             (protocols/original instance))))))

(deftest ^:parallel override-default-fields-test
  (testing "should still be able to override default fields"
    (is (= {:select [:id :name], :from [[:venues]]}
           (pipeline/build :toucan.query-type/select.* ::venues.default-fields {:columns [:id :name]} {})))
    (is (= (instance/instance ::venues.default-fields
                              {:id 1, :name "Tempest"})
           (select/select-one [::venues.default-fields :id :name] {:order-by [[:id :asc]]})))
    (testing `select/select-one-fn
      (is (= (java.time.LocalDateTime/parse "2017-01-01T00:00")
             (select/select-one-fn :created-at ::venues.default-fields 1))))))

(deftest ^:synchronized insert-returning-instances-test
  (test/with-discarded-table-changes :venues
    (is (= [(instance/instance ::venues.default-fields {:id 4, :name "BevLess", :category "store"})]
           (insert/insert-returning-instances! ::venues.default-fields [{:name "BevLess", :category "store"}])))))

(derive ::venues.anaphor ::test/venues)
(derive ::venues.anaphor.id ::venues.anaphor)
(derive ::venues.anaphor.category ::venues.anaphor)

(default-fields/define-default-fields ::venues.anaphor
  (case &model
    ::venues.anaphor.id       [:id :name]
    ::venues.anaphor.category [:category :name]))

(deftest ^:parallel anaphor-test
  (testing "define-default-fields should introduce an &model anaphor"
    (are [model expected] (= expected
                             (select/select-one model 1))
      ::venues.anaphor.id       {:name "Tempest", :id 1}
      ::venues.anaphor.category {:name "Tempest", :category "bar"})))

(derive ::venues.default-fields-arbitrary-fns ::test/venues)

(default-fields/define-default-fields ::venues.default-fields-arbitrary-fns
  [:id
   [(comp inc :id) ::incremented-id]
   :name])

(deftest ^:parallel arbitrary-fns-test
  (testing "Should work with arbitrary functions"
    (is (= {:id              1
            ::incremented-id 2
            :name            "Tempest"}
           (select/select-one ::venues.default-fields-arbitrary-fns 1)))))

(derive ::venues.add-price ::venues.default-fields)

(after-select/define-after-select ::venues.add-price
  [venue]
  (assoc venue :price "$$$"))

(deftest ^:parallel after-select-with-default-fields-test
  (testing "default-fields should work in combination with after-select"
    (is (= {:id 1, :name "Tempest", :category "bar", :price "$$$"}
           (select/select-one ::venues.add-price 1)))))

(derive ::venues.with-created-at ::venues.default-fields)

(default-fields/define-default-fields ::venues.with-created-at
  [:id :name :category :created-at])

(deftest ^:parallel override-less-specific-impl-test
  (testing "Should be able to override less-specific :default-fields"
    (is (= {:id         1
            :name       "Tempest"
            :category   "bar"
            :created-at (java.time.LocalDateTime/parse "2017-01-01T00:00"),}
           (select/select-one ::venues.with-created-at 1)))))
