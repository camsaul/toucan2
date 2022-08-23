(ns toucan2.tools.after-select-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.protocols :as protocols]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.helpers :as helpers]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(after-select/define-after-select ::people [person]
  (assoc person ::after-select? true))

(derive ::venues.short-name ::test/venues)

(after-select/define-after-select ::venues.short-name
  [venue]
  (assoc venue :short-name (str/join (take 4 (:name venue)))))

(deftest columns-added-by-after-select-should-not-be-considered-part-of-changes-test
  (let [instance (select/select-one ::venues.short-name 1)]
    (is (= (instance/instance ::venues.short-name
                              {:id         1
                               :name       "Tempest"
                               :short-name "Temp"
                               :category   "bar"
                               :created-at (LocalDateTime/parse "2017-01-01T00:00")
                               :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           instance))
    (is (nil? (protocols/changes instance)))))

(derive ::venues.short-name-with-transforms ::venues.short-name)

(helpers/deftransforms ::venues.short-name-with-transforms
  {:name {:in  identity
          :out identity}})

(deftest after-select-should-not-affect-insert-test
  (doseq [model [::venues.short-name
                 ::venues.short-name-with-transforms]]
    (testing model
      (is (= [(instance/instance model
                                 {:id 1, :name "Tempest", :short-name "Temp"})
              (instance/instance model
                                 {:id 2, :name "Ho's Tavern", :short-name "Ho's"})
              (instance/instance model
                                 {:id 3, :name "BevMo", :short-name "BevM"})]
             (select/select [model :id :name] {:order-by [[:id :asc]]})))
      (test/with-discarded-table-changes :venues
        (is (= 1
               (insert/insert! model :name "Tin Vietnamese", :category "restaurant"))))
      (test/with-discarded-table-changes :venues
        (is (= [4]
               (insert/insert-returning-pks! model :name "Tin Vietnamese", :category "restaurant"))))
      (test/with-discarded-table-changes :venues
        (is (= [(instance/instance model
                                   {:id 4, :name "Tin Vietnamese", :short-name "Tin ", :category "restaurant"})]
               (insert/insert-returning-instances! [model :id :name :category]
                                                   :name "Tin Vietnamese"
                                                   :category "restaurant")))))))

(deftest after-select-should-not-affect-update-test
  (doseq [model [::venues.short-name
                 ::venues.short-name-with-transforms]]
    (testing model
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! model 3 {:name "BevLess"}))))
      (test/with-discarded-table-changes :venues
        (is (= [3]
               (update/update-returning-pks! model 3 {:name "BevLess"})))))))
