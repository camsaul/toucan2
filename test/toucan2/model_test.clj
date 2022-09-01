(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]))

(deftest ^:parallel primary-keys-test
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

(deftest ^:parallel primary-keys-wrap-results-test
  (testing "primary-keys should wrap results in a vector if needed, and validate"
    (is (= [:name]
           (model/primary-keys ::primary-keys.returns-bare-keyword)))
    (is (= [:name :id]
           (model/primary-keys ::primary-keys.returns-vector)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Bad toucan2.model/primary-keys for model .* should return keyword or sequence of keywords, got .*"
         (model/primary-keys ::primary-keys.returns-invalid)))))

(deftest ^:parallel ^:parallel table-name-test
  (doseq [[model expected] {"ABC"    "ABC"
                            :abc     "abc"
                            :ns/abc  "abc"
                            :default "default"
                            'symbol  "symbol"}]
    (testing (pr-str `(model/table-name ~model))
      (is (= expected
             (model/table-name model))))))

(derive ::people.unquoted ::test/people)

(m/defmethod pipeline/transduce-with-model [#_query-type :default #_model ::people.unquoted]
  [rf query-type model parsed-args]
  (binding [map.honeysql/*options* {:quoted false}]
    (next-method rf query-type model parsed-args)))

(deftest ^:parallel default-honeysql-options-for-a-model-test
  (testing "There should be a way to specify 'default options' for a specific model"
    (is (= ["SELECT * FROM people WHERE id = ?" 1]
           (tools.compile/compile
             (select/select ::people.unquoted {:select [:*]
                                               :from   [[:people]]
                                               :where  [:= :id 1]}))))))

;;; [[model/default-connectable]] gets tested basically everywhere, because we define it for the models in
;;; [[toucan2.test]] and use it in almost every test namespace

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::venues.namespaced :venue
   ::test/categories   :category})

(deftest ^:parallel model->namespace-test
  (are [model expected] (= expected
                           (model/model->namespace model))
    ::venues.namespaced {::venues.namespaced :venue, ::test/categories :category}
    :venues             nil
    nil                 nil))

(deftest ^:parallel table-name->namespace-test
  (are [model expected] (= expected
                           (model/table-name->namespace model))
    ::venues.namespaced {"venues" :venue, "category" :category}
    :venues             nil
    nil                 nil))

(derive ::venues.namespaced.child ::venues.namespaced)

(deftest ^:parallel namespace-test
  (are [model expected] (= expected
                           (model/namespace model))
    ::venues.namespaced       :venue
    ::venues.namespaced.child :venue
    :venues                   nil
    nil                       nil))

(deftest ^:parallel namespaced-default-primary-keys-test
  (is (= [:venue/id]
         (model/primary-keys ::venues.namespaced))))
