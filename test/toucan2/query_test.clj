(ns toucan2.query-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.query :as query]))

(deftest ^:parallel condition->honeysql-where-clause-test
  (doseq [[[k v] expected] {[:id :id]           [:= :id :id]
                            [1 :id]             [:= 1 :id]
                            [:id 1]             [:= :id 1]
                            [:a 1]              [:= :a 1]
                            [:id [:> 1]]        [:> :id 1]
                            [:a [:between 1 2]] [:between :a 1 2]}]
    (testing (pr-str `(query/condition->honeysql-where-clause ~k ~v))
      (is (= expected
             (query/condition->honeysql-where-clause k v))))))

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
