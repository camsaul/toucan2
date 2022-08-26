(ns toucan2.tools.identity-query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.identity-query :as identity-query]))

(deftest query-test
  (is (= [[1 2]
          [:a :b]]
         (execute/query (identity-query/identity-query [[1 2] [:a :b]])))))

(def ^:private parrot-query
  (identity-query/identity-query [{:id 1, :name "Parroty"}
                                  {:id 2, :name "Green Friend"}]))

(deftest select-test
  (is (= [(instance/instance ::parrot {:id 1, :name "Parroty"})
          (instance/instance ::parrot {:id 2, :name "Green Friend"})]
         (select/select ::parrot parrot-query))))

(m/defmethod query/do-with-resolved-query [:default ::parrot-query]
  [_model _queryable f]
  (f (identity-query/identity-query [{:id 1, :name "Parroty"}
                                     {:id 2, :name "Green Friend"}])))

(deftest named-query-test
  (is (= [(instance/instance ::parrot {:id 1, :name "Parroty"})
          (instance/instance ::parrot {:id 2, :name "Green Friend"})]
         (select/select ::parrot ::parrot-query))))

(deftest select-test-2
  (is (= [(instance/instance ::birb {:id 1, :name "Parroty"})
          (instance/instance ::birb {:id 2, :name "Green Friend"})]
         (select/select ::birb parrot-query))))

(deftest select-fn-test
  (is (= #{"Parroty" "Green Friend"}
         (select/select-fn-set :name ::birb parrot-query))))

(deftest select-one-test
  (is (= [(instance/instance ::venues {:id 1, :name "No Category", :category nil})]
         (select/select ::venues (identity-query/identity-query
                                  [{:id 1, :name "No Category", :category nil}])))))

(after-select/define-after-select ::my-after-select
  [instance]
  (assoc instance :after-select? true))

(defn- do-after-select [model rows]
  (select/select model (identity-query/identity-query rows)))

(defn- do-after-reducible-select [model rows]
  (pipeline/reducible-with-model :toucan.query-type/select.instances
                                 model
                                 {:queryable (identity-query/identity-query rows)}))

(deftest do-after-select-test
  (testing "Can we use `identity-query` to build some sort of abomination like Toucan 1 do-post-select?"
    (let [reducible (select/reducible-select [::test/venues :id :name] {:order-by [[:id :asc]], :limit 2})]
      (is (= [(instance/instance ::my-after-select {:id 1, :name "Tempest", :after-select? true})
              (instance/instance ::my-after-select {:id 2, :name "Ho's Tavern", :after-select? true})]
             (do-after-select ::my-after-select reducible)
             (into [] (do-after-reducible-select ::my-after-select reducible)))))))

(deftest identity-query-as-model-test
  (testing "Can we use identity-query as an 'identity model'?"
    (let [query   (identity-query/identity-query [{:a 1, :b 2} {:a 3, :b 4}])
          results (select/select query)]
      (is (= [{:a 1, :b 2}
              {:a 3, :b 4}]
             results))
      (testing "Return plain rows, not instances"
        (is (not (instance/instance? (first results))))))))
