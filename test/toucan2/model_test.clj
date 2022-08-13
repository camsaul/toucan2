(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.model :as model]))

(deftest ^:parallel default-table-name-test
  (doseq [[model expected] {"ABC"   "ABC"
                            :abc    "abc"
                            :ns/abc "abc"}]
    (testing (pr-str `(model/table-name ~model))
      (is (= expected
             (model/table-name model))))))

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
