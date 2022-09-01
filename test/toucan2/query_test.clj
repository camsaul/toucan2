(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.test :as test]))

(deftest ^:parallel default-parse-args-test
  (are [args expected] (= expected
                          (query/parse-args :default args))
    [:model]                       {:modelable :model, :queryable {}}
    [:model {}]                    {:modelable :model, :queryable {}}
    [:model nil]                   {:modelable :model, :queryable nil}
    [[:model] {}]                  {:modelable :model, :queryable {}}
    [[:model :a :b :c] {}]         {:modelable :model, :columns [:a :b :c], :queryable {}}
    [:model :k :v]                 {:modelable :model, :kv-args {:k :v}, :queryable {}}
    [:model :k :v {}]              {:modelable :model, :kv-args {:k :v}, :queryable {}}
    [:conn :db :model]             {:connectable :db, :modelable :model, :queryable {}}
    [:conn :db :model :query]      {:connectable :db, :modelable :model, :queryable :query}
    [:conn :db [:model]]           {:connectable :db, :modelable :model, :queryable {}}
    [:conn :db [:model :a] :query] {:connectable :db, :modelable :model, :columns [:a], :queryable :query}
    [:conn :db :model :id 1]       {:connectable :db, :modelable :model, :kv-args {:id 1}, :queryable {}}))

(derive ::venues.compound-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.compound-pk
  [_model]
  [:id :name])

(deftest ^:parallel toucan-pk-vector-forms-test
  (testing ":toucan/pk should work with vector forms like `:in`"
    (are [model arg expected] (= {:select [:*]
                                  :from   [[:venues]]
                                  :where  expected}
                                 (pipeline/build :toucan.query-type/select.instances
                                                 model
                                                 {:kv-args {:toucan/pk arg}}
                                                 {}))
      ::test/venues        4                                    [:= :id 4]
      ::test/venues        [4]                                  [:= :id 4]
      ::test/venues        [:> 4]                               [:> :id 4]
      ::test/venues        [:in [4]]                            [:in :id [4]]
      ::test/venues        [:in [4 5]]                          [:in :id [4 5]]
      ::test/venues        [:between 4 5]                       [:between :id 4 5]
      ::venues.compound-pk [4 "BevMo"]                          [:and [:= :id 4] [:= :name "BevMo"]]
      ::venues.compound-pk [:> [4 "BevMo"]]                     [:and [:> :id 4] [:> :name "BevMo"]]
      ::venues.compound-pk [:in [[4 "BevMo"]]]                  [:and [:in :id [4]] [:in :name ["BevMo"]]]
      ::venues.compound-pk [:in [[4 "BevMo"] [5 "BevLess"]]]    [:and [:in :id [4 5]] [:in :name ["BevMo" "BevLess"]]]
      ::venues.compound-pk [:between [4 "BevMo"] [5 "BevLess"]] [:and [:between :id 4 5] [:between :name "BevMo" "BevLess"]])))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::test/venues :venue})

(deftest ^:parallel namespaced-toucan-pk-test
  (is (= {:select [:*]
          :from   [[:venues :venue]]
          :where  [:= :venue/id 1]}
         (pipeline/build :toucan.query-type/select.instances
                         ::venues.namespaced
                         {:kv-args {:toucan/pk 1}}
                         {}))))
