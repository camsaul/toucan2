(ns toucan2.tools.with-temp-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.with-temp :as with-temp]))

(use-fixtures
  :each
  test/do-db-types-fixture
  (fn [thunk]
    (is (= 6
           (select/count ::test/birds)))
    (test/with-discarded-table-changes :birds
      (thunk)
      (testing "after with-temp body"
        (is (= 6
               (select/count ::test/birds)))))))

(deftest no-attributes-test
  (with-temp/with-temp [::test/birds bird]
    (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
           bird))
    (is (= 7
           (select/count ::test/birds)))))

(deftest explicit-attributes-test
  (with-temp/with-temp [::test/birds bird {:name "Green Enemy"}]
    (is (= (instance/instance ::test/birds {:id 7, :name "Green Enemy", :bird-type "parrot", :good-bird nil})
           bird))
    (is (= 7
           (select/count ::test/birds)))))

(deftest multiple-objects-test
  (testing "multiple objects, referencing one another"
    (with-temp/with-temp [::test/birds bird-1 nil
                          ::test/birds bird-2 {:name (str (:name bird-1) " 2.0")}]
      (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
             bird-1))
      (is (= (instance/instance ::test/birds {:id 8, :name "birb 2.0", :bird-type "parrot", :good-bird nil})
             bird-2))
      (is (= 8
             (select/count ::test/birds))))))

(derive ::birds.with-default-type ::test/birds)

(m/defmethod with-temp/with-temp-defaults ::birds.with-default-type
  [_model]
  {:bird-type "parakeet"})

(deftest custom-defaults-test
  (testing "Use the with-temp-defaults"
    (with-temp/with-temp [::birds.with-default-type bird]
      (is (= (instance/instance ::birds.with-default-type
                                {:id 7, :name "birb", :bird-type "parakeet", :good-bird nil})
             bird))
      (is (= 7
             (select/count ::birds.with-default-type))))))

(deftest explicit-attributes-override-defaults-test
  (testing "explicit attributes passed to `with-temp` should override defaults"
    (with-temp/with-temp [::birds.with-default-type bird {:bird-type "toucan"}]
      (is (= (instance/instance ::birds.with-default-type
                                {:id 7, :name "birb", :bird-type "toucan", :good-bird nil})
             bird))
      (is (= 7
             (select/count ::birds.with-default-type))))))

(m/defmethod model/do-with-model ::unresolved-model
  [_modelable f]
  (f ::test/birds))

(deftest resolve-model-test
  (testing "with-temp should resolve models"
    (with-temp/with-temp [::unresolved-model bird]
      (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
             bird)))))

(derive ::birds.custom-with-temp ::test/birds)

(def ^:private custom-do-with-temp-called? (atom false))

(m/defmethod with-temp/do-with-temp* :before ::birds.custom-with-temp
  [_model _explicit-attributes f]
  (reset! custom-do-with-temp-called? true)
  f)

(deftest custom-do-with-temp-test
  (reset! custom-do-with-temp-called? false)
  (with-temp/with-temp [::birds.custom-with-temp bird]
    (is (= (instance/instance ::birds.custom-with-temp {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
           bird)))
  (is @custom-do-with-temp-called?))

(deftest error-test
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       (case (test/current-db-type)
         :postgres #"ERROR: duplicate key value violates unique constraint"
         :h2       #"Unique index or primary key violation")
       (with-temp/with-temp [::test/birds bird {:id 1}]
         (is (= :not-here
                bird)
             "should never get here.")))))
