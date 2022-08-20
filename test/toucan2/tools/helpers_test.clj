(ns toucan2.tools.helpers-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(use-fixtures :each test/do-db-types-fixture)

(derive ::people ::test/people)

(helpers/define-before-select ::people
  [args]
  (assoc args :columns [:id :name]))

(helpers/define-after-select-each ::people [person]
  (assoc person ::after-select? true))

(deftest select-helpers-test
  (is (= (instance/instance ::people {:id             1
                                      :name           "Cam"
                                      ::after-select? true})
         (select/select-one ::people 1))))

(derive ::venues.short-name ::test/venues)

(helpers/define-after-select-each ::venues.short-name
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
    (is (nil? (instance/changes instance)))))

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

(derive ::venues.default-fields ::test/venues)

(helpers/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest define-default-fields-test
  (is (= {:select [:id :name :category], :from [[:venues]]}
         (query/build ::select/select ::venues.default-fields {:query {}})))
  (is (= [(instance/instance ::venues.default-fields
                             {:id 1, :name "Tempest", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 2, :name "Ho's Tavern", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 3, :name "BevMo", :category "store"})]
         (select/select ::venues.default-fields)))
  (testing "should still be able to override default fields"
    (is (= {:select [:id :name], :from [[:venues]]}
           (query/build ::select/select ::venues.default-fields {:query {}, :columns [:id :name]})))
    (is (= (instance/instance ::venues.default-fields
                              {:id 1, :name "Tempest"})
           (select/select-one [::venues.default-fields :id :name] {:order-by [[:id :asc]]})))))

(derive ::venues.before-delete ::test/venues)

(def ^:dynamic ^:private *deleted-venues*)

(helpers/define-before-delete ::venues.before-delete
  [venue]
  (when *deleted-venues*
    (swap! *deleted-venues* conj venue))
  nil)

(deftest before-delete-test
  (test/with-discarded-table-changes :venues
    (binding [*deleted-venues* (atom [])]
      (is (= 2
             (delete/delete! ::venues.before-delete :category "bar")))
      (is (= [(instance/instance ::venues.before-delete
                                 {:id 3, :name "BevMo", :category "store"})]
             (select/select [::venues.before-delete :id :name :category])))
      (is (= [(instance/instance ::venues.before-delete
                                 {:id         1
                                  :name       "Tempest"
                                  :category   "bar"
                                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
              (instance/instance ::venues.before-delete
                                 {:id         2
                                  :name       "Ho's Tavern"
                                  :category   "bar"
                                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
             @*deleted-venues*)))))

(derive ::venues.before-delete-exception ::test/venues)

(helpers/define-before-delete ::venues.before-delete-exception
  [venue]
  (update/update! ::test/venues (:id venue) {:updated-at (LocalDateTime/parse "2022-08-16T14:22:00")})
  (when (= (:category venue) "store")
    (throw (ex-info "Don't delete a store!" {:venue venue}))))

(deftest before-delete-exception-test
  (testing "exception in before-delete"
    (test/with-discarded-table-changes :venues
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Don't delete a store"
           (delete/delete! ::venues.before-delete-exception)))
      (testing "Should be done inside a transaction"
        (is (= [(instance/instance ::venues.before-delete-exception
                                   {:id         1
                                    :name       "Tempest"
                                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                (instance/instance ::venues.before-delete-exception
                                   {:id         2
                                    :name       "Ho's Tavern"
                                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                (instance/instance ::venues.before-delete-exception
                                   {:id         3
                                    :name       "BevMo"
                                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
               (select/select [::venues.before-delete-exception :id :name :updated-at]
                              {:order-by [[:id :asc]]})))))))

;;;; Tools for [[helpers/deftransforms]] now live in [[toucan2.tools.transformed-test]]
