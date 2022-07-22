(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.realize :as realize]
   [toucan2.test :as test]))

(deftest reducible-query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")})]
         (realize/realize (model/reducible-query-as ::test/db :people "SELECT * FROM people WHERE id = 1;")))))

(deftest query-as-test
  (is (= [(instance/instance :people {:id 1, :name "Cam"})
          (instance/instance :people {:id 2, :name "Sam"})
          (instance/instance :people {:id 3, :name "Pam"})
          (instance/instance :people {:id 4, :name "Tam"})]
         (model/query-as ::test/db :people {:select [:id :name], :from [:people]}))))

(deftest ^:parallel default-table-name-test
  (doseq [[model expected] {"ABC"   "ABC"
                            :abc    "abc"
                            :ns/abc "abc"}]
    (testing (pr-str `(model/table-name ~model))
      (is (= expected
             (model/table-name model))))))

(deftest ^:parallel condition->honeysql-where-clause-test
  (doseq [[[k v] expected] {[:id :id]           [:= :id :id]
                            [1 :id]             [:= 1 :id]
                            [:id 1]             [:= :id 1]
                            [:a 1]              [:= :a 1]
                            [:id [:> 1]]        [:> :id 1]
                            [:a [:between 1 2]] [:between :a 1 2]}]
    (testing (pr-str `(model/condition->honeysql-where-clause ~k ~v))
      (is (= expected
             (model/condition->honeysql-where-clause k v))))))

(deftest ^:parallel default-build-select-query-test
  (is (= {:select [:*]
          :from   [[:default]]}
         (model/build-select-query :default {} nil nil)))
  (testing "don't override existing"
    (is (= {:select [:a :b]
            :from   [[:my_table]]}
           (model/build-select-query :default {:select [:a :b], :from [[:my_table]]} nil nil))))
  (testing "columns"
    (is (= {:select [:a :b]
            :from   [[:default]]}
           (model/build-select-query :default {} [:a :b] nil)))
    (testing "existing"
      (is (= {:select [:a]
              :from   [[:default]]}
             (model/build-select-query :default {:select [:a]} [:a :b] nil)))))
  (testing "conditions"
    (is (= {:select [:*]
            :from   [[:default]]
            :where  [:= :id 1]}
           (model/build-select-query :default {} nil {:id 1})))
    (testing "merge with existing"
      (is (= {:select [:*]
              :from   [[:default]]
              :where  [:and [:= :a :b] [:= :id 1]]}
             (model/build-select-query :default {:where [:= :a :b]} nil {:id 1}))))))

(deftest built-in-pk-condition-test
  (is (= {:select [:*], :from [[:default]], :where [:= :id 1]}
         (model/build-select-query :default {} nil {:toucan2/pk 1})))
  (is (= {:select [:*], :from [[:default]], :where [:and
                                                    [:= :name "Cam"]
                                                    [:= :id 1]]}
         (model/build-select-query :default {:where [:= :name "Cam"]} nil {:toucan2/pk 1}))))

(m/defmethod model/apply-condition [:default clojure.lang.IPersistentMap ::custom.limit]
  [_model honeysql-form _k limit]
  (assoc honeysql-form :limit limit))

(deftest custom-condition-test
  (is (= {:select [:*]
          :from   [[:default]]
          :limit  100}
         (model/build-select-query :default {} nil {::custom.limit 100})
         (model/build-select-query :default {:limit 1} nil {::custom.limit 100}))))

(m/defmethod model/do-with-model :toucan2.model-test.quoted/people
  [modelable f]
  (binding [compile/*honeysql-options* {:quoted true}]
    (next-method modelable f)))

(deftest default-honeysql-options-for-a-model-test
  (testing "There should be a way to specify 'default options' for a specific model"
    (model/with-model [_model :toucan2.model-test.quoted/people]
      (compile/with-compiled-query [query [:default
                                           {:select [:*]
                                            :from   [[:toucan2.model-test.quoted/people]]
                                            :where  [:= :id 1]}]]
        ;; this is what HoneySQL normally does with a namespaced keyword
        (is (= ["SELECT * FROM \"toucan2.model_test.quoted\".\"people\" WHERE \"id\" = ?" 1]
               query))))))
