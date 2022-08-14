(ns toucan2.tools.identity-query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
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
