(ns toucan2.tools.default-fields-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.after-insert :as after-insert]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.tools.named-query :as named-query]
   [toucan2.tools.transformed :as transformed])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues.default-fields ::test.track-realized/venues)

(default-fields/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest ^:parallel select-test
  (testing "In Toucan 2, default fields SHOULD NOT rewrite the query."
    (is (= {:select [:*], :from [[:venues]]}
           (pipeline/build :toucan.query-type/select.instances ::venues.default-fields {} {}))))
  (test.track-realized/with-realized-columns [realized-columns]
    (is (= [(instance/instance ::venues.default-fields
                               {:id 1, :name "Tempest", :category "bar"})
            (instance/instance ::venues.default-fields
                               {:id 2, :name "Ho's Tavern", :category "bar"})
            (instance/instance ::venues.default-fields
                               {:id 3, :name "BevMo", :category "store"})]
           (select/select ::venues.default-fields)))
    (is (= #{:venues/name :venues/id :venues/category}
           (realized-columns))
        "Only realize the specific columns we've asked for.")))

(deftest ^:parallel preserve-model-test
  (testing "Should preserve the model"
    (let [instance (select/select-one ::venues.default-fields 1)]
      (is (= {:id 1, :name "Tempest", :category "bar"}
             instance))
      (is (instance/instance? instance))
      (is (instance/instance-of? ::venues.default-fields instance))
      (is (= ::venues.default-fields
             (protocols/model instance)))
      (is (= nil
             (protocols/changes instance)))
      (is (= (into {} instance)
             (protocols/original instance))))))

(deftest ^:parallel override-default-fields-test
  (testing "should still be able to override default fields"
    (is (= {:select [:venues/id :venues/name], :from [[:venues]]}
           (pipeline/build :toucan.query-type/select.* ::venues.default-fields {:columns [:id :name]} {})))
    (is (= (instance/instance ::venues.default-fields
                              {:id 1, :name "Tempest"})
           (select/select-one [::venues.default-fields :id :name] {:order-by [[:id :asc]]})))
    (testing `select/select-one-fn
      (is (= (java.time.LocalDateTime/parse "2017-01-01T00:00")
             (select/select-one-fn :created-at ::venues.default-fields 1))))))

(deftest ^:synchronized insert-returning-instances-test
  (test/with-discarded-table-changes :venues
    (is (= [(instance/instance ::venues.default-fields {:id 4, :name "BevLess", :category "store"})]
           (insert/insert-returning-instances! ::venues.default-fields [{:name "BevLess", :category "store"}])))))

(derive ::venues.anaphor ::test/venues)
(derive ::venues.anaphor.id ::venues.anaphor)
(derive ::venues.anaphor.category ::venues.anaphor)

(default-fields/define-default-fields ::venues.anaphor
  (case &model
    ::venues.anaphor.id       [:id :name]
    ::venues.anaphor.category [:category :name]))

(deftest ^:parallel anaphor-test
  (testing "define-default-fields should introduce an &model anaphor"
    (are [model expected] (= expected
                             (select/select-one model 1))
      ::venues.anaphor.id       {:name "Tempest", :id 1}
      ::venues.anaphor.category {:name "Tempest", :category "bar"})))

(derive ::venues.default-fields-arbitrary-fns ::test/venues)

(default-fields/define-default-fields ::venues.default-fields-arbitrary-fns
  [:id
   [(comp inc :id) ::incremented-id]
   :name])

(deftest ^:parallel arbitrary-fns-test
  (testing "Should work with arbitrary functions"
    (is (= {:id              1
            ::incremented-id 2
            :name            "Tempest"}
           (select/select-one ::venues.default-fields-arbitrary-fns 1)))))

(derive ::venues.default-fields.after-select ::venues.default-fields)

(after-select/define-after-select ::venues.default-fields.after-select
  [venue]
  (assoc venue :price "$$$"))

(deftest ^:synchronized after-select-with-default-fields-test
  (testing "default-fields should work in combination with after-select. Preserve fields added by after-select."
    (is (= {:id 1, :name "Tempest", :category "bar", :price "$$$"}
           (select/select-one ::venues.default-fields.after-select 1)))
    (testing `insert/insert-returning-instances!
      (test/with-discarded-table-changes :venues
        (is (= [{:id 4, :name "Savoy Tivoli", :category "bar", :price "$$$"}]
               (insert/insert-returning-instances! ::venues.default-fields.after-select
                                                   {:name "Savoy Tivoli", :category "bar"})))))))

(derive ::venues.default-fields.after-insert ::venues.default-fields)

(after-insert/define-after-insert ::venues.default-fields.after-insert
  [venue]
  (assoc venue :price "$$$"))

(deftest ^:synchronized after-insert-with-default-fields-test
  (testing "default-fields should work in combination with after-insert. Preserve fields added by after-insert."
    (testing "Sanity check: should not be present in results of select"
      (is (= {:id 1, :name "Tempest", :category "bar"}
             (select/select-one ::venues.default-fields.after-insert 1))))
    (testing `insert/insert-returning-instances!
      (test/with-discarded-table-changes :venues
        (is (= [{:id 4, :name "Savoy Tivoli", :category "bar", :price "$$$"}]
               (insert/insert-returning-instances! ::venues.default-fields.after-insert
                                                   {:name "Savoy Tivoli", :category "bar"})))))))

(derive ::venues.default-fields.transformed ::venues.default-fields)

(transformed/deftransforms ::venues.default-fields.transformed
  {:name {:in identity, :out str/upper-case}})

(deftest ^:synchronized transformed-default-fields-test
  (testing "default-fields should work in combination with transformed."
    (is (= {:id 1, :name "TEMPEST", :category "bar"}
           (select/select-one ::venues.default-fields.transformed 1)))
    (testing `insert/insert-returning-instances!
      (test/with-discarded-table-changes :venues
        (is (= [{:id 4, :name "SAVOY TIVOLI", :category "bar"}]
               (insert/insert-returning-instances! ::venues.default-fields.transformed
                                                   {:name "Savoy Tivoli", :category "bar"})))))))

(derive ::venues.default-fields.after-select.transformed ::venues.default-fields.after-select)
(derive ::venues.default-fields.after-select.transformed ::venues.default-fields.transformed)

(deftest ^:synchronized after-select-transformed-default-fields-test
  (testing "default-fields should work in combination with after-select AND transformed."
    (is (= {:id 1, :name "TEMPEST", :category "bar", :price "$$$"}
           (select/select-one ::venues.default-fields.after-select.transformed 1)))
    (testing `insert/insert-returning-instances!
      (test/with-discarded-table-changes :venues
        (is (= [{:id 4, :name "SAVOY TIVOLI", :category "bar", :price "$$$"}]
               (insert/insert-returning-instances! ::venues.default-fields.after-select.transformed
                                                   {:name "Savoy Tivoli", :category "bar"})))))))

(derive ::venues.with-created-at ::venues.default-fields)

(default-fields/define-default-fields ::venues.with-created-at
  [:id :name :category :created-at])

(deftest ^:parallel override-less-specific-impl-test
  (testing "Should be able to override less-specific :default-fields"
    (is (= {:id         1
            :name       "Tempest"
            :category   "bar"
            :created-at (java.time.LocalDateTime/parse "2017-01-01T00:00")}
           (select/select-one ::venues.with-created-at 1)))))

(named-query/define-named-query ::named-query.select-venues-override-default-fields
  {:select [:id :name :updated-at], :where [:= :id 1]})

(deftest ^:parallel do-not-apply-default-fields-in-query-with-select
  (testing "Don't apply default-fields if we specify a Honey SQL query with explicit"
    (doseq [select [:select :select-distinct]]
      (testing select
        (are [query] (= {:id 1, :name "Tempest", :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                        (select/select-one ::venues.default-fields query))
          {select [:id :name :updated-at], :where [:= :id 1]}
          ::named-query.select-venues-override-default-fields)))))

(deftest ^:parallel macroexpansion-test
  (testing "define-default-fields should define vars with different names based on the model."
    (letfn [(generated-name* [form]
              (cond
                (sequential? form)
                (some generated-name* form)

                (and (symbol? form)
                     (str/starts-with? (name form) "default-fields")) form))
            (generated-name [form]
              (let [expanded (walk/macroexpand-all form)]
                (or (generated-name* expanded)
                    ['no-match expanded])))]
      (is (= 'default-fields-primary-method-model-1
             (generated-name `(default-fields/define-default-fields :model-1
                                [~'venue]
                                ~'venue))))
      (is (= 'default-fields-primary-method-model-2
             (generated-name `(default-fields/define-default-fields :model-2
                                [~'venue]
                                ~'venue)))))))

(deftest ^:synchronized hierarchy-validity-test
  (testing "define-default-fields should maintain valid hierarchies when models derive from each other"
    (testing "metabase scenario: derive child from parent, then define-default-fields on both"
      (with-redefs [clojure.core/global-hierarchy (make-hierarchy)]
      (let [parent ::test-parent-model
            child ::test-child-model]
        (derive child parent)
        (default-fields/define-default-fields child
          [:id :name])
        (default-fields/define-default-fields parent
          [:id :category])
        (is (isa? child parent))
        (is (isa? child ::default-fields/default-fields))
        (is (isa? parent ::default-fields/default-fields))
        (is (not (contains? (parents child) ::default-fields/default-fields))
            "child should not have ::default-fields as a direct parent")
        (underive parent ::default-fields/default-fields)
        (underive child ::default-fields/default-fields)
          (underive child parent))))))
