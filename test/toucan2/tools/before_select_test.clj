(ns toucan2.tools.before-select-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.before-select :as before-select]))

(set! *warn-on-reflection* true)

(derive ::people.id-and-name ::test/people)

(before-select/define-before-select ::people.id-and-name
  [args]
  (assoc args :columns [:id :name]))

(after-select/define-after-select ::people.id-and-name [person]
  (assoc person ::after-select? true))

(deftest ^:parallel before-select-test
  (is (= (instance/instance ::people.id-and-name {:id             1
                                                  :name           "Cam"
                                                  ::after-select? true})
         (select/select-one ::people.id-and-name 1))))

(derive ::people.named-cam ::test/people)

(before-select/define-before-select ::people.named-cam
  [args]
  (assoc-in args [:kv-args :name] "Cam"))

(deftest ^:parallel add-kv-args-test
  (is (= [(instance/instance ::people.named-cam {:id 1, :name "Cam"})]
         (select/select [::people.named-cam :id :name]))))

(doto ::people.named-cam.id-and-name
  (derive ::people.named-cam)
  (derive ::people.id-and-name))

(deftest ^:parallel compose-test
  (is (= [(instance/instance ::people.named-cam.id-and-name {:id 1, :name "Cam", ::after-select? true})]
         (select/select ::people.named-cam.id-and-name))))

(derive ::people.increment-id ::test/people)

(def ^:private ^:dynamic *select-calls* nil)

(before-select/define-before-select ::people.increment-id
  [args]
  (when *select-calls*
    (swap! *select-calls* conj (:kv-args args)))
  (update-in args [:kv-args :id] inc))

(deftest ^:parallel only-call-once-test
  (testing "before-select method should be applied exactly once"
    (binding [*select-calls* (atom [])]
      (is (= {:id 2, :name "Sam"}
             (select/select-one [::people.increment-id :id :name] :id 1)))
      (is (= [{:id 1}]
             @*select-calls*)))))
