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

(derive ::people ::test/people)

(before-select/define-before-select ::people
  [args]
  (assoc args :columns [:id :name]))

(after-select/define-after-select ::people [person]
  (assoc person ::after-select? true))

(deftest before-select-test
  (is (= (instance/instance ::people {:id             1
                                      :name           "Cam"
                                      ::after-select? true})
         (select/select-one ::people 1))))
