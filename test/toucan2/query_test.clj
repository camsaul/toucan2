(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.test :as test]))

(deftest default-parse-args-test
  (are [args expected] (= expected
                          (query/parse-args :default args))
    [:model]               {:modelable :model, :queryable {}}
    [:model {}]            {:modelable :model, :queryable {}}
    [:model nil]           {:modelable :model, :queryable nil}
    [[:model] {}]          {:modelable :model, :queryable {}}
    [[:model :a :b :c] {}] {:modelable :model, :columns [:a :b :c], :queryable {}}
    [:model :k :v]         {:modelable :model, :kv-args {:k :v}, :queryable {}}
    [:model :k :v {}]      {:modelable :model, :kv-args {:k :v}, :queryable {}}))

(deftest ^:parallel condition->honeysql-where-clause-test
  (doseq [[[k v] expected] {[:id :id]           [:= :id :id]
                            [1 :id]             [:= 1 :id]
                            [:id 1]             [:= :id 1]
                            [:a 1]              [:= :a 1]
                            [:id [:> 1]]        [:> :id 1]
                            [:a [:between 1 2]] [:between :a 1 2]
                            [:id [:in 1 2]]     [:in :id [1 2]]}]
    (testing (pr-str `(query/condition->honeysql-where-clause ~k ~v))
      (is (= expected
             (query/condition->honeysql-where-clause k v))))))

(m/defmethod query/do-with-resolved-query [:default ::named-query]
  [_model _queryable f]
  (f {:select [[:%count.* :count]], :from [:venues]}))

(deftest with-resolved-query-test
  (let [executed-body? (atom false)]
    (query/with-resolved-query [resolved-query [nil ::named-query]]
      (reset! executed-body? true)
      (is (= {:select [[:%count.* :count]], :from [:venues]}
             resolved-query)))
    (is @executed-body?))
  (testing "detect errors"
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand-1 `(query/with-resolved-query ~'resolved-query ::named-query))))))

(deftest build-test
  (is (= {:where [:= :a 1]}
         (query/build ::my-query-type nil {:query {}, :kv-args {:a 1}}))))

(m/defmethod model/primary-keys ::model-with-non-id-pk
  [_model]
  [:uuid])

(deftest build-query-for-int-test
  (testing "Raw integer PK as query"
    (is (= {:where [:= :id 1]}
           (query/build ::my-query-type nil {:query 1})
           (query/build ::my-query-type nil {:kv-args {:toucan/pk 1}}))))
  (testing "custom non-:id PK"
    (is (= {:where [:= :uuid 1]}
           (query/build ::my-query-type ::model-with-non-id-pk {:query 1})
           (query/build ::my-query-type ::model-with-non-id-pk {:kv-args {:toucan/pk 1}})))))

(deftest plain-sql-query-test
  (doseq [query ["SELECT *"
                 ["SELECT *"]]]
    (testing (pr-str query)
      (is (= ["SELECT *"]
             (query/build nil nil {:query query})))
      (testing "disallow kv-args"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"key-value args are not supported for plain SQL queries"
             (query/build nil nil {:query query, :kv-args {:toucan/pk 1}})))))))

(derive ::venues.compound-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.compound-pk
  [_model]
  [:id :name])

(deftest toucan-pk-vector-forms-test
  (testing ":toucan/pk should work with vector forms like `:in`"
    (are [model arg expected] (= {:select [:*]
                                  :from   [[:venues]]
                                  :where  expected}
                                 (query/build :toucan2.query.type/select.returning-instances
                                              model
                                              {:kv-args {:toucan/pk arg}, :query {}}))
      ::test/venues        4                                    [:= :id 4]
      ::test/venues        [:> 4]                               [:> :id 4]
      ::test/venues        [:in [4]]                            [:in :id [4]]
      ::test/venues        [:in [4 5]]                          [:in :id [4 5]]
      ::test/venues        [:between 4 5]                       [:between :id 4 5]
      ::venues.compound-pk [4 "BevMo"]                          [:and [:= :id 4] [:= :name "BevMo"]]
      ::venues.compound-pk [:> [4 "BevMo"]]                     [:and [:> :id 4] [:> :name "BevMo"]]
      ::venues.compound-pk [:in [[4 "BevMo"]]]                  [:and [:in :id [4]] [:in :name ["BevMo"]]]
      ::venues.compound-pk [:in [[4 "BevMo"] [5 "BevLess"]]]    [:and [:in :id [4 5]] [:in :name ["BevMo" "BevLess"]]]
      ::venues.compound-pk [:between [4 "BevMo"] [5 "BevLess"]] [:and [:between :id 4 5] [:between :name "BevMo" "BevLess"]])))
