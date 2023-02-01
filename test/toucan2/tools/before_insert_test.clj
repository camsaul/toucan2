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
   [toucan2.tools.named-query :as tools.named-query]
   [toucan2.tools.transformed :as transformed])
  (:import
   (java.time LocalDateTime OffsetDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues.before-insert ::test/venues)

(before-insert/define-before-insert ::venues.before-insert
  [venue]
  (cond-> venue
    (:name venue) (update :name str/upper-case)))

(deftest ^:synchronized before-insert-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (insert/insert! ::venues.before-insert {:name "Tin Vietnamese", :category "restaurant"})))
    (is (= (instance/instance ::venues.before-insert {:id 4, :name "TIN VIETNAMESE"})
           (select/select-one [::venues.before-insert :id :name] :id 4)))))

(derive ::venues.composed ::venues.before-insert)

(before-insert/define-before-insert ::venues.composed
  [venue]
  (update venue :category str/upper-case))

(deftest ^:synchronized compose-before-insert-test
  (testing "If a model has multiple before-inserts, they should compose"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (insert/insert! ::venues.composed {:name "Tin Vietnamese", :category "restaurant"})))
      (is (= (instance/instance ::venues.composed {:id 4, :name "TIN VIETNAMESE", :category "RESTAURANT"})
             (select/select-one [::venues.composed :id :name :category] :id 4))))))

(derive ::venues.serialized-category ::test/venues)

(transformed/deftransforms ::venues.serialized-category
  {:category {:in  pr-str
              :out edn/read-string}})

(before-insert/define-before-insert ::venues.serialized-category
  [{:keys [category], :as venue}]
  (assert (and (map? category)
               (string? (:name category)))
          (format "Invalid category: %s" (pr-str category)))
  (assoc-in venue [:category :validated?] true))

(deftest ^:synchronized happens-before-transforms-test
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

(tools.named-query/define-named-query ::named-rows
  {:rows [{:name "Grant & Green", :category "bar"}
          {:name "North Beach Cantina", :category "restaurant"}]})

(deftest ^:synchronized named-query-test
  (doseq [insert! [#'insert/insert!
                   #'insert/insert-returning-pks!
                   #'insert/insert-returning-instances!]]
    (test/with-discarded-table-changes :venues
      (testing insert!
        (is (= (condp = insert!
                 #'insert/insert!                     2
                 #'insert/insert-returning-pks!       [4 5]
                 #'insert/insert-returning-instances! [(instance/instance
                                                        ::venues.before-insert
                                                        {:id         4
                                                         :name       "GRANT & GREEN"
                                                         :category   "bar"
                                                         :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                         :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                                                       (instance/instance
                                                        ::venues.before-insert
                                                        {:id         5
                                                         :name       "NORTH BEACH CANTINA"
                                                         :category   "restaurant"
                                                         :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                         :updated-at (LocalDateTime/parse "2017-01-01T00:00")})])
               (insert! ::venues.before-insert ::named-rows)))))))

(derive ::people.suffix-name ::test/people)

(def ^:private ^:dynamic *inserted-people* nil)

(before-insert/define-before-insert ::people.suffix-name
  [person]
  (when *inserted-people*
    (swap! *inserted-people* conj (:name person)))
  (cond-> person
    (:name person) (update :name #(str % " 2.0"))))

(deftest ^:synchronized only-call-once-test
  (test/with-discarded-table-changes :people
    (testing "before-insert method should be applied exactly once"
      (binding [*inserted-people* (atom [])]
        (is (= [5]
               (insert/insert-returning-pks! ::people.suffix-name {:name "CAM"})))
        (is (= ["CAM"]
               @*inserted-people*))
        (is (= {:name "CAM 2.0"}
               (select/select-one [::people.suffix-name :name] 5)))))))

(derive ::venues.exception.clojure-land ::test/venues)

(before-insert/define-before-insert ::venues.exception.clojure-land
  [venue]
  (insert/insert! ::test/venues {:name "ANOTHER STORE", :category "bar"})
  ;; trigger a Clojure-land error
  (when (= (:category venue) "store")
    (throw (ex-info "Don't insert a store!" {:venue venue})))
  venue)

(derive ::venues.exception.db-land ::test/venues)

(before-insert/define-before-insert ::venues.exception.db-land
  [venue]
  (insert/insert! ::test/venues {:name "ANOTHER STORE", :category "bar"})
  ;; trigger a DB-land error
  (cond-> venue
    (= (:category venue) "store") (assoc :id 1)))

(deftest ^:synchronized exception-test
  (doseq [model [::venues.exception.clojure-land
                 ::venues.exception.db-land]]
    (testing (format "Model = %s" model)
      (testing "\nexception in before-insert"
        (test/with-discarded-table-changes :venues
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               (case model
                 ::venues.exception.clojure-land #"Don't insert a store"
                 ::venues.exception.db-land      (case (test/current-db-type)
                                                   :postgres #"ERROR: duplicate key value violates unique constraint"
                                                   :h2       #"Unique index or primary key violation"
                                                   :mariadb  #"Duplicate entry"))
               (insert/insert! model {:category "store", :name "My Store"})))
          (testing "\nShould be done inside a transaction"
            (is (= [(instance/instance model
                                       {:id         1
                                        :name       "Tempest"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance model
                                       {:id         2
                                        :name       "Ho's Tavern"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance model
                                       {:id         3
                                        :name       "BevMo"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   (select/select [model :id :name :updated-at]
                                  {:order-by [[:id :asc]]})))))))))

(derive ::people.default-values ::test/people)

(before-insert/define-before-insert ::people.default-values
  [person]
  (merge
   {:name       "Default Person"
    :created_at (OffsetDateTime/parse "2022-12-31T17:26:00-08:00")}
   person))

(deftest ^:synchronized default-values-test
  (test/with-discarded-table-changes :people
    (is (= 1
           (insert/insert! ::people.default-values {})))
    (is (= {:id         5
            :name       "Default Person"
            :created-at (case (test/current-db-type)
                          :h2                  (OffsetDateTime/parse "2022-12-31T17:26:00-08:00")
                          (:postgres :mariadb) (OffsetDateTime/parse "2023-01-01T01:26Z"))}
           (select/select-one ::people.default-values 5)))))
