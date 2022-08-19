(ns toucan2.tools.identity-query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
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

(m/defmethod query/do-with-query [:default ::parrot-query]
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

(m/defmethod select/select-reducible* :after ::select-reducible-identity-query
  [_model _reducible-query]
  (identity-query/identity-query [{:a 1, :b 2}
                                  {:a 3, :b 4}]))

(deftest identity-query-in-select-reducible-test
  (testing "Can we have select-reducible* return an identity query, and have things still work?"
    (is (= [{:a 1, :b 2}
            {:a 3, :b 4}]
           (select/select ::select-reducible-identity-query)))))

(m/defmethod select/select-reducible* :after ::wrap-reducible-query
  [_model _reducible-query]
  (identity-query/identity-query (select/select-reducible [::test/venues :id :name] {:order-by [[:id :asc]], :limit 2})))

(deftest wrap-reducible-query-test
  (testing "Can identity-query wrap another reducible query?"
    (is (= [(instance/instance ::test/venues {:id 1, :name "Tempest"})
            (instance/instance ::test/venues {:id 2, :name "Ho's Tavern"})]
           (select/select ::wrap-reducible-query)))))

(helpers/define-after-select-each ::my-after-select
  [instance]
  (assoc instance :after-select? true))

(defn- do-after-select [model rows]
  (select/select model (identity-query/identity-query rows)))

(defn- do-after-select-reducible [model rows]
  (select/select-reducible* model {:query (identity-query/identity-query rows)}))

(deftest do-after-select-test
  (testing "Can we use `identity-query` to build some sort of abomination like Toucan 1 do-post-select?"
    (let [reducible (select/select-reducible [::test/venues :id :name] {:order-by [[:id :asc]], :limit 2})]
      (is (= [(instance/instance ::my-after-select {:id 1, :name "Tempest", :after-select? true})
              (instance/instance ::my-after-select {:id 2, :name "Ho's Tavern", :after-select? true})]
             (do-after-select ::my-after-select reducible)
             (into [] (do-after-select-reducible ::my-after-select reducible)))))))

(deftest identity-query-as-model-test
  (testing "Can we use identity-query as an 'identity model'?"
    (let [query   (identity-query/identity-query [{:a 1, :b 2} {:a 3, :b 4}])
          results (select/select query)]
      (is (= [{:a 1, :b 2}
              {:a 3, :b 4}]
             results))
      (testing "Return plain rows, not instances"
        (is (not (instance/instance? (first results))))))))
