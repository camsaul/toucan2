(ns toucan2.tools.helpers-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

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

(derive ::venues.default-fields ::test/venues)

(helpers/define-default-fields ::venues.default-fields
  [:id :name :category])

(deftest define-default-fields-test
  (is (= [(instance/instance ::venues.default-fields
                             {:id 1, :name "Tempest", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 2, :name "Ho's Tavern", :category "bar"})
          (instance/instance ::venues.default-fields
                             {:id 3, :name "BevMo", :category "store"})]
         (select/select ::venues.default-fields))))



;;; TODO -- should `after-update` automatically do things in a transaction? So if `after-update` fails, the original
;;; updates were canceled?

(derive ::venues.before-insert ::test/venues)

(helpers/define-before-insert ::venues.before-insert
  [venue]
  (cond-> venue
    (:name venue) (update :name str/upper-case)))


(deftest before-insert-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (insert/insert! ::venues.before-insert {:name "Tin Vietnamese", :category "resturaunt"})))
    (is (= (instance/instance ::venues.before-insert {:id 4, :name "TIN VIETNAMESE"})
           (select/select-one [::venues.before-insert :id :name] :id 4)))))

(derive ::venues.after-insert ::test/venues)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(helpers/define-after-insert ::venues.after-insert
  [venue]
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj venue))
  (assoc venue :awaiting-moderation? true))

(deftest after-insert-test
  (doseq [f [#'insert/insert!
             #'insert/insert-returning-keys!
             #'insert/insert-returning-instances!]]
    (testing f
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (is (= (condp = f
                   #'insert/insert! 1
                   #'insert/insert-returning-keys! [4]
                   #'insert/insert-returning-instances! [{:id                   4
                                                          :name                 "Lombard Heights Market"
                                                          :category             "liquor-store"
                                                          :created-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                          :updated-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                          :awaiting-moderation? true}])
                 (f ::venues.after-insert {:name "Lombard Heights Market", :category "liquor-store"})))
          (testing "should be added to *venues-awaiting-moderation*"
            (is (= [(instance/instance
                     ::venues.after-insert
                     {:id         4
                      :name       "Lombard Heights Market"
                      :category   "liquor-store"
                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   @*venues-awaiting-moderation*))))))))

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
               (select/select [::venues.before-delete-exception :id :name :updated-at] {:order-by [[:id :asc]]})))))))

(derive ::transformed-venues ::test/venues)

(helpers/deftransforms ::transformed-venues
  {:id {:in  #(some-> % Integer/parseInt)
        :out str}})

(deftest deftransforms-test
  (is (= (instance/instance
          ::transformed-venues
          {:id         "1"
           :name       "Tempest"
           :category   "bar"
           :created-at (LocalDateTime/parse "2017-01-01T00:00")
           :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::transformed-venues :toucan/pk "1"))))
