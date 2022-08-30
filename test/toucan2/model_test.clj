(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(deftest primary-keys-test
  (testing "default implementation"
    (is (= [:id]
           (model/primary-keys ::some-default-model)))))

(m/defmethod model/primary-keys ::primary-keys.returns-bare-keyword
  [_model]
  :name)

(m/defmethod model/primary-keys ::primary-keys.returns-vector
  [_model]
  [:name :id])

(m/defmethod model/primary-keys ::primary-keys.returns-invalid
  [_model]
  [:name nil])

(deftest primary-keys-wrap-results-test
  (testing "primary-keys should wrap results in a vector if needed, and validate"
    (is (= [:name]
           (model/primary-keys ::primary-keys.returns-bare-keyword)))
    (is (= [:name :id]
           (model/primary-keys ::primary-keys.returns-vector)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Bad toucan2.model/primary-keys for model .* should return keyword or sequence of keywords, got .*"
         (model/primary-keys ::primary-keys.returns-invalid)))))

(deftest ^:parallel table-name-test
  (doseq [[model expected] {"ABC"    "ABC"
                            :abc     "abc"
                            :ns/abc  "abc"
                            :default "default"
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

(m/defmethod model/model->namespace ::venues
  [_model]
  {::test/venues     :venue
   ::test/categories :category})

(deftest model->namespace-test
  (are [model expected] (= expected
                           (model/model->namespace model))
    ::venues {::test/venues :venue, ::test/categories :category}
    :venues  nil
    nil      nil))

(deftest table-name->namespace-test
  (are [model expected] (= expected
                           (model/table-name->namespace model))
    ::venues {"venues" :venue, "category" :category}
    :venues  nil
    nil      nil))
