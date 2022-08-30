(ns toucan2.tools.before-select-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.before-select :as before-select]))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(derive ::people.id-and-name ::test/people)

(before-select/define-before-select ::people.id-and-name
  [args]
  (assoc args :columns [:id :name]))

(after-select/define-after-select ::people.id-and-name [person]
  (assoc person ::after-select? true))

(deftest before-select-test
  (is (= (instance/instance ::people.id-and-name {:id             1
                                      :name           "Cam"
                                      ::after-select? true})
         (select/select-one ::people.id-and-name 1))))

(derive ::people.named-cam ::test/people)

(before-select/define-before-select ::people.named-cam
  [args]
  (assoc-in args [:kv-args :name] "Cam"))

(deftest add-kv-args-test
  (is (= [(instance/instance ::people.named-cam {:id 1, :name "Cam"})]
         (select/select [::people.named-cam :id :name]))))

(doto ::people.named-cam.id-and-name
  (derive ::people.named-cam)
  (derive ::people.id-and-name))

(deftest compose-test
  (is (= [(instance/instance ::people.named-cam.id-and-name {:id 1, :name "Cam", ::after-select? true})]
         (select/select ::people.named-cam.id-and-name))))
