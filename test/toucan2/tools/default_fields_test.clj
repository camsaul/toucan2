(ns toucan2.tools.default-fields-test
  (:require
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.default-fields :as default-fields]))

(set! *warn-on-reflection* true)

(derive ::venues.default-fields ::test.track-realized/venues)

(default-fields/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest ^:parallel select-test
  (is (= {:select [:id :name :category], :from [[:venues]]}
         (pipeline/build :toucan.query-type/select.instances ::venues.default-fields {} {})))
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
        "Only realize the specific columns we've asked for."))
  (testing "should still be able to override default fields"
    (is (= {:select [:id :name], :from [[:venues]]}
           (pipeline/build :toucan.query-type/select.* ::venues.default-fields {:columns [:id :name]} {})))
    (is (= (instance/instance ::venues.default-fields
                              {:id 1, :name "Tempest"})
           (select/select-one [::venues.default-fields :id :name] {:order-by [[:id :asc]]})))))

(deftest insert-returning-instances-test
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
