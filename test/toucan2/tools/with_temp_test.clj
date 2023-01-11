(ns toucan2.tools.with-temp-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.with-temp :as with-temp]))

(defn- do-with-temp-test [thunk]
  (test/with-discarded-table-changes "birds"
    (is (= 6
           (select/count ::test/birds)))
    (thunk)
    (testing "after with-temp body"
      (is (= 6
             (select/count ::test/birds))))))

(deftest ^:parallel valid-syntax-test
  (are [form] (seqable? (macroexpand-1 form))
    `(with-temp/with-temp [model] :ok)
    `(with-temp/with-temp [model ~'_] :ok)
    `(with-temp/with-temp [model ~'instance {} model] :ok)
    `(with-temp/with-temp [model {:keys [~'a]}] :ok)
    `(with-temp/with-temp [model {:keys [~'a]} nil] :ok)
    `(with-temp/with-temp [model {:keys [~'a]} {}] :ok)
    `(with-temp/with-temp [model ~'instance {} model] :ok)
    `(with-temp/with-temp [model ~'instance {} model ~'_] :ok)
    `(with-temp/with-temp [model ~'instance {} model ~'instance] :ok)
    `(with-temp/with-temp [model ~'instance {} model ~'instance])))

(deftest ^:parallel valid-syntax-test-2
  (testing "Disallow nil models"
    (are [form] (thrown?
                 clojure.lang.Compiler$CompilerException
                 (macroexpand-1 form))
      `(with-temp/with-temp [] :ok)
      `(with-temp/with-temp [nil] :ok)
      `(with-temp/with-temp [:model a {} nil] :ok))
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand-1 `(with-temp/with-temp [] :ok))))
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand-1 `(with-temp/with-temp [nil] :ok)))))
  (testing "model binding must be a valid binding form"
    (are [form] (thrown?
                 clojure.lang.Compiler$CompilerException
                 (macroexpand-1 form))
      `(with-temp/with-temp [:model 100] :ok)
      `(with-temp/with-temp [:model 100 nil] :ok)
      `(with-temp/with-temp [:model a nil :model 200] :ok))))

(deftest ^:parallel validate-attributes-test
  (is (thrown-with-msg?
       Throwable
       #"Assert failed: attributes passed to toucan2.tools.with-temp/with-temp must be a map"
       (with-temp/with-temp [::test/venues _ 123]
         (is (= :not-here
                :here)
             "should never get here.")))))

(deftest ^:synchronized no-attributes-test
  (do-with-temp-test
   (fn []
     (with-temp/with-temp [::test/birds bird]
       (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
              bird))
       (is (= 7
              (select/count ::test/birds)))))))

(deftest ^:synchronized destructuring-test
  (do-with-temp-test
   (fn []
     (with-temp/with-temp [::test/birds {bird-name :name}]
       (is (= "birb"
              bird-name))))))

(deftest ^:synchronized explicit-attributes-test
  (do-with-temp-test
   (fn []
     (with-temp/with-temp [::test/birds bird {:name "Green Enemy"}]
       (is (= (instance/instance ::test/birds {:id 7, :name "Green Enemy", :bird-type "parrot", :good-bird nil})
              bird))
       (is (= 7
              (select/count ::test/birds)))))))

(deftest ^:synchronized no-binding-test
  (do-with-temp-test
   (fn []
     (testing "Should work with just a model, and no instance binding"
       (with-temp/with-temp [::test/birds]
         (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
                (select/select-one ::test/birds :id 7)))
         (is (= 7
                (select/count ::test/birds))))))))

(deftest ^:synchronized multiple-objects-test
  (do-with-temp-test
   (fn []
     (testing "multiple objects, referencing one another"
       (with-temp/with-temp [::test/birds bird-1 nil
                             ::test/birds bird-2 {:name (str (:name bird-1) " 2.0")}]
         (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
                bird-1))
         (is (= (instance/instance ::test/birds {:id 8, :name "birb 2.0", :bird-type "parrot", :good-bird nil})
                bird-2))
         (is (= 8
                (select/count ::test/birds))))))))

(derive ::birds.with-default-type ::test/birds)

(m/defmethod with-temp/with-temp-defaults ::birds.with-default-type
  [_model]
  {:bird-type "parakeet"})

(deftest ^:synchronized custom-defaults-test
  (do-with-temp-test
   (fn []
     (testing "Use the with-temp-defaults"
       (with-temp/with-temp [::birds.with-default-type bird]
         (is (= (instance/instance ::birds.with-default-type
                                   {:id 7, :name "birb", :bird-type "parakeet", :good-bird nil})
                bird))
         (is (= 7
                (select/count ::birds.with-default-type))))))))

(deftest ^:synchronized explicit-attributes-override-defaults-test
  (do-with-temp-test
   (fn []
     (testing "explicit attributes passed to `with-temp` should override defaults"
       (with-temp/with-temp [::birds.with-default-type bird {:bird-type "toucan"}]
         (is (= (instance/instance ::birds.with-default-type
                                   {:id 7, :name "birb", :bird-type "toucan", :good-bird nil})
                bird))
         (is (= 7
                (select/count ::birds.with-default-type))))))))

(m/defmethod pipeline/transduce-with-model [#_query-type :default #_model ::unresolved-model]
  [rf query-type _model parsed-args]
  (next-method rf query-type ::test/birds parsed-args))

(deftest ^:synchronized resolve-model-test
  (do-with-temp-test
   (fn []
     (testing "with-temp should resolve models"
       (with-temp/with-temp [::unresolved-model bird]
         (is (= (instance/instance ::test/birds {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
                bird)))))))

(derive ::birds.custom-with-temp ::test/birds)

(def ^:private custom-do-with-temp-called? (atom false))

(m/defmethod with-temp/do-with-temp* :before ::birds.custom-with-temp
  [_model _explicit-attributes f]
  (reset! custom-do-with-temp-called? true)
  f)

(deftest ^:synchronized custom-do-with-temp-test
  (do-with-temp-test
   (fn []
     (reset! custom-do-with-temp-called? false)
     (with-temp/with-temp [::birds.custom-with-temp bird]
       (is (= (instance/instance ::birds.custom-with-temp {:id 7, :name "birb", :bird-type "parrot", :good-bird nil})
              bird)))
     (is @custom-do-with-temp-called?))))

(deftest ^:synchronized error-test
  (do-with-temp-test
   (fn []
     (is (thrown-with-msg?
          clojure.lang.ExceptionInfo
          (case (test/current-db-type)
            :postgres #"ERROR: duplicate key value violates unique constraint"
            :h2       #"Unique index or primary key violation")
          (with-temp/with-temp [::test/birds bird {:id 1}]
            (is (= :not-here
                   bird)
                "should never get here.")))))))
