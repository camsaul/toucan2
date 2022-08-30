(ns toucan2.tools.default-fields-test
  (:require
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.default-fields :as default-fields]))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(derive ::venues.default-fields ::test/venues)

(default-fields/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest select-test
  (is (= {:select [:id :name :category], :from [[:venues]]}
         (query/build :toucan.query-type/select.instances ::venues.default-fields {:query {}})))
  (is (= [(instance/instance ::venues.default-fields
                             {:id 1, :name "Tempest", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 2, :name "Ho's Tavern", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 3, :name "BevMo", :category "store"})]
         (select/select ::venues.default-fields)))
  (testing "should still be able to override default fields"
      (is (= {:select [:id :name], :from [[:venues]]}
             (query/build :toucan.query-type/select.* ::venues.default-fields {:query {}, :columns [:id :name]})))
      (is (= (instance/instance ::venues.default-fields
                                {:id 1, :name "Tempest"})
             (select/select-one [::venues.default-fields :id :name] {:order-by [[:id :asc]]})))))

(deftest insert-returning-instances-test
  (test/with-discarded-table-changes :venues
    (is (= [(instance/instance ::venues.default-fields {:id 4, :name "BevLess", :category "store"})]
           (insert/insert-returning-instances! ::venues.default-fields [{:name "BevLess", :category "store"}])))))
