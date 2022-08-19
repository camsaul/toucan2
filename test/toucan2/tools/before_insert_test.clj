(ns toucan2.tools.before-insert-test
  (:require
   [clojure.edn :as edn]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.before-insert :as before-insert]
   [toucan2.tools.helpers :as helpers])
  (:import
   (java.time LocalDateTime)))

(use-fixtures :each test/do-db-types-fixture)

(derive ::venues.before-insert ::test/venues)

(before-insert/define-before-insert ::venues.before-insert
  [venue]
  (cond-> venue
    (:name venue) (update :name str/upper-case)))

(deftest before-insert-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (insert/insert! ::venues.before-insert {:name "Tin Vietnamese", :category "restaurant"})))
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
             (insert/insert! ::venues.composed {:name "Tin Vietnamese", :category "restaurant"})))
      (is (= (instance/instance ::venues.composed {:id 4, :name "TIN VIETNAMESE", :category "RESTAURANT"})
             (select/select-one [::venues.composed :id :name :category] :id 4))))))

(derive ::venues.serialized-category ::test/venues)

(helpers/deftransforms ::venues.serialized-category
  {:category {:in  pr-str
              :out edn/read-string}})

(before-insert/define-before-insert ::venues.serialized-category
  [{:keys [category], :as venue}]
  (assert (and (map? category)
               (string? (:name category)))
          (format "Invalid category: %s" (pr-str category)))
  (assoc-in venue [:category :validated?] true))

(deftest happens-before-transforms-test
  (testing "before-insert has to happen *before* any transforms.\n"
    (doseq [insert! [#'insert/insert!
                     #'insert/insert-returning-pks!
                     #'insert/insert-returning-instances!]]
      (test/with-discarded-table-changes :venues
        (testing insert!
          (let [expected {:id         4
                          :name       "Tin Vietnamese"
                          :category   {:name "restaurant", :validated? true}
                          :created-at (LocalDateTime/parse "2017-01-01T00:00")
                          :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
            (is (= (condp = insert!
                     #'insert/insert!                     1
                     #'insert/insert-returning-pks!       [4]
                     #'insert/insert-returning-instances! [expected])
                   (insert! ::venues.serialized-category {:name "Tin Vietnamese", :category {:name "restaurant"}})))
            (is (= expected
                   (select/select-one ::venues.serialized-category :id 4)))))))))
