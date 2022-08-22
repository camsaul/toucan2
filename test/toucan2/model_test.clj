(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(deftest ^:parallel table-name-test
  (doseq [[model expected] {"ABC"    "ABC"
                            :abc     "abc"
                            :ns/abc  "abc"
                            :default "default"!
                            'symbol  "symbol"}]
    (testing (pr-str `(model/table-name ~model))
      (is (= expected
             (model/table-name model))))))

(derive ::people.quoted ::test/people)

(m/defmethod model/do-with-model ::people.quoted
  [modelable f]
  (binding [compile/*honeysql-options* {:quoted true}]
    (next-method modelable f)))

(deftest default-honeysql-options-for-a-model-test
  (testing "There should be a way to specify 'default options' for a specific model"
    (model/with-model [_model ::people.quoted]
      (compile/with-compiled-query [query [nil {:select [:*]
                                                :from   [[:people]]
                                                :where  [:= :id 1]}]]
        ;; this is what HoneySQL normally does with a namespaced keyword
        (is (= ["SELECT * FROM \"people\" WHERE \"id\" = ?" 1]
               query))))))

;;; [[model/default-connectable]] gets tested basically everywhere, because we define it for the models in
;;; [[toucan2.test]] and use it in almost every test namespace
