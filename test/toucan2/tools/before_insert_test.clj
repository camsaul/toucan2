(ns toucan2.tools.before-insert-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.before-insert :as before-insert]))

(derive ::venues.before-insert ::test/venues)

(before-insert/define-before-insert ::venues.before-insert
  [venue]
  (cond-> venue
    (:name venue) (update :name str/upper-case)))


(deftest before-insert-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (insert/insert! ::venues.before-insert {:name "Tin Vietnamese", :category "resturaunt"})))
    (is (= (instance/instance ::venues.before-insert {:id 4, :name "TIN VIETNAMESE"})
           (select/select-one [::venues.before-insert :id :name] :id 4)))))

(derive ::venues.composed ::venues.before-insert)

(before-insert/define-before-insert ::venues.composed
  [venue]
  (update venue :category str/upper-case))

(deftest compose-before-insert-test
  (testing "If a model has multiple before-inserts, they should compose"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (insert/insert! ::venues.composed {:name "Tin Vietnamese", :category "resturaunt"})))
      (is (= (instance/instance ::venues.composed {:id 4, :name "TIN VIETNAMESE", :category "RESTURAUNT"})
             (select/select-one [::venues.composed :id :name :category] :id 4))))))
