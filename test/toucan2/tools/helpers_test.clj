(ns toucan2.tools.helpers-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
   [toucan2.update :as update]
   [toucan2.delete :as delete])
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

(def ^:dynamic ^:private *updated-venues* nil)

(derive ::venues.before-update ::test/venues)

(helpers/define-before-update ::venues.before-update
  [venue]
  (is (instance/instance? venue))
  (is (isa? (instance/model venue) ::venues.before-update))
  (when *updated-venues*
    (swap! *updated-venues* conj venue))
  venue)

(derive ::venues.update-updated-at ::venues.before-update)

(helpers/define-before-update ::venues.update-updated-at
  [venue]
  (assoc venue :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")))

(derive ::venues.discard-category-change ::venues.before-update)

(helpers/define-before-update ::venues.discard-category-change
  [venue]
  (assoc venue :category (:category (instance/original venue))))

(deftest before-update-test
  (testing "Updates returned by the before-update method should actually be applied"
    (testing "f adds changes"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! ::venues.update-updated-at 1 {:name "Kennedy's Irish Pub and Curry House"})))
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")}
               (select/select-one ::venues.update-updated-at 1)))))
    (testing "f discards changes"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! ::venues.discard-category-change 1 {:name     "Kennedy's Irish Pub and Curry House"
                                                                   :category "bar-plus-curry-house"})))
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
               (select/select-one ::venues.discard-category-change 1)))))))

(deftest before-update-pk-condition-test
  (testing "define-before-update should call its method with all objects that match update conditions"
    (testing "PK condition"
      (test/with-discarded-table-changes :venues
        (binding [*updated-venues* (atom [])]
          (is (= 1
                 (update/update! ::venues.before-update 1 {:name "Kennedy's Irish Pub and Curry House"})))
          (is (= "Kennedy's Irish Pub and Curry House"
                 (select/select-one-fn :name ::venues.before-update 1)))
          (testing "current"
            (is (= [(instance/instance ::venues.before-update {:id         1
                                                               :name       "Kennedy's Irish Pub and Curry House"
                                                               :category   "bar"
                                                               :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                               :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   @*updated-venues*)))
          (testing "original"
            (is (= [(instance/instance ::venues.before-update {:id         1
                                                               :name       "Tempest"
                                                               :category   "bar"
                                                               :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                               :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   (map instance/original @*updated-venues*))))
          (testing "changes"
            (is (= [{:name "Kennedy's Irish Pub and Curry House"}]
                   (map instance/changes @*updated-venues*)))))))))

(deftest before-update-non-pk-condition-test
  (testing "define-before-update should call its method with all objects that match update conditions"
    (testing "non-PK condition"
      (test/with-discarded-table-changes :venues
        (binding [*updated-venues* (atom [])]
          (is (= 2
                 (update/update! ::venues.before-update :category [:not= "store"] {:category "not-store"})))
          (is (= "not-store"
                 (select/select-one-fn :category ::venues.before-update 1)
                 (select/select-one-fn :category ::venues.before-update 2)))
          (testing "current"
            (is (= [{:id         1
                     :name       "Tempest"
                     :category   "not-store"
                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                    {:id         2
                     :name       "Ho's Tavern"
                     :category   "not-store"
                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
                   (sort-by :id @*updated-venues*))))
          (testing "changes"
            (is (= [{:category "not-store"}
                    {:category "not-store"}]
                   (map instance/changes @*updated-venues*)))))))))

(deftest before-update-pk-and-other-conditons-test
  (testing "define-before-update should call its method with all objects that match update conditions"
    (testing "PK + other conditions"
      (test/with-discarded-table-changes :venues
        (binding [*updated-venues* (atom [])]
          (is (= 1
                 (update/update! ::venues.before-update 3 :category [:not= "bar"] {:category "not-bar"})))
          (is (= "not-bar"
                 (select/select-one-fn :category ::venues.before-update 3)))
          (testing "current"
            (is (= [{:id         3
                     :name       "BevMo"
                     :category   "not-bar"
                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
                   @*updated-venues*)))
          (testing "changes"
            (is (= [{:category "not-bar"}]
                   (map instance/changes @*updated-venues*)))))))))

(deftest before-update-no-changes-test
  (testing "No changes passed to update!* itself -- should no-op"
    (test/with-discarded-table-changes :venues
      (execute/with-call-count [call-count]
        (is (zero? (update/update! ::venues.before-update 1 {})))
        (testing "Should have no calls -- no changes, no need to update anything."
          (is (zero? (call-count)))))))

  (testing "No matching instances -- should no-op"
    (test/with-discarded-table-changes :venues
      (execute/with-call-count [call-count]
        (is (zero? (update/update! ::venues.before-update :category "museum" {:name "A Museum"})))
        (testing "Should have just one call (initial select to find matching values)"
          (is (= 1
                 (call-count)))))))

  (testing "No changes relative to the actual object in the DB -- should no-op"
    (testing "[before f]"
      (test/with-discarded-table-changes :venues
        (execute/with-call-count [call-count]
          (is (zero? (update/update! ::venues.before-update 1 {:name "Tempest"})))
          (testing "Should have just one call (initial select to find matching values)"
            (is (= 1
                   (call-count)))))))
    (testing "[after f]"
      (test/with-discarded-table-changes :venues
        (execute/with-call-count [call-count]
          (is (zero? (update/update! ::venues.discard-category-change 1 {:category "dive-bar"})))
          (testing "Should have just one call (initial select to find matching values)"
            (is (= 1
                   (call-count)))))))))

(def ^:private ^:dynamic *venues-update-queries* nil)

(derive ::venues.capture-updates ::venues.before-update)

(m/defmethod query/build [::update/update ::venues.capture-updates :default]
  [query-type model parsed-args]
  (when *venues-update-queries*
    (swap! *venues-update-queries* conj parsed-args))
  (next-method query-type model parsed-args))

;;; this changes the category to `category-<id>` with the venue ID
(derive ::venues.add-unique-category ::venues.capture-updates)

(helpers/define-before-update ::venues.add-unique-category
  [venue]
  (assoc venue :category (format "category-%d" (:id venue))))

(deftest before-update-batch-updates-one-batch-test
  (testing "Group together updates into one call per set of updates."
    (testing "Only one update needed"
      (test/with-discarded-table-changes :venues
        (execute/with-call-count [call-count]
          (binding [*venues-update-queries* (atom [])]
            (is (= 2
                   (update/update! ::venues.capture-updates :category "bar" {:category "dive-bar"})))
            (testing "Should have 2 DB calls -- one to fetch matching rows, another to do the single update"
              (is (= 2
                     (call-count)))
              (testing "Don't add extra :where clauses if there's just one set of changes to apply to all matching rows."
                (is (= [{:kv-args {:category "bar"}
                         :changes {:category "dive-bar"}
                         :query   {}}]
                       @*venues-update-queries*))))))))))

(deftest before-update-batch-updates-multiple-batches-test
  (testing "Group together updates into one call per set of updates."
    (testing "Multiple updates needed"
      (test/with-discarded-table-changes :venues
        (execute/with-call-count [call-count]
          (binding [*venues-update-queries* (atom [])]
            (is (= 2
                   (update/update! ::venues.add-unique-category :category "bar"
                                   {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")})))
            (testing "Should have 3 DB calls -- one to fetch matching rows, then 2 separate updates"
              (is (= 3
                     (call-count)))
              (is (= [{:changes {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
                                 :category   "category-1"}
                       :query   {}
                       :kv-args {:id 1}}
                      {:changes {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
                                 :category   "category-2"}
                       :query   {}
                       :kv-args {:id 2}}]
                     @*venues-update-queries*))
              (is (= [(instance/instance ::test/venues
                                         {:id         1
                                          :name       "Tempest"
                                          :category   "category-1"
                                          :updated-at (LocalDateTime/parse "2021-06-09T15:18")})
                      (instance/instance ::test/venues {:id         2
                                                        :name       "Ho's Tavern"
                                                        :category   "category-2"
                                                        :updated-at (LocalDateTime/parse "2021-06-09T15:18")})
                      (instance/instance ::test/venues {:id         3
                                                        :name       "BevMo"
                                                        :category   "store"
                                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                     (select/select [::test/venues :id :name :category :updated-at] {:order-by [[:id :asc]]}))))))))))

;;; TODO -- need a test that we do some number of update batches BETWEEN 1..n for n affected rows. e.g. we affect 3 rows
;;; with 2 sets of changes, should only do 2 updates.

(deftest before-update-transaction-test
  (testing "before-update should run in a transaction; an error during some part should cause all updates to be discarded"
    ;; TODO
    ))

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(derive ::venues.after-update ::test/venues)

(helpers/define-after-update ::venues.after-update
  [venue]
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj venue))
  nil)

(deftest after-update-test
  (doseq [f [#'update/update!
             #'update/update-returning-pks!]]
    (testing f
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (is (= (condp = f
                   #'update/update!               2
                   #'update/update-returning-pks! [1 2])
                 (f ::venues.after-update :category "bar" {:category "BARRR"})))
          (testing "rows should be updated in DB"
            (is (= [(instance/instance ::venues.after-update {:id 1, :name "Tempest", :category "BARRR"})
                    (instance/instance ::venues.after-update {:id 2, :name "Ho's Tavern", :category "BARRR"})]
                   (select/select [::venues.after-update :id :name :category] :category "BARRR" {:order-by [[:id :asc]]}))))
          (testing (str "rows should have been added to " `*venues-awaiting-moderation*)
            (is (= [(instance/instance ::venues.after-update {:id         1
                                                              :name       "Tempest"
                                                              :category   "BARRR"
                                                              :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance ::venues.after-update {:id         2
                                                              :name       "Ho's Tavern"
                                                              :category   "BARRR"
                                                              :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   @*venues-awaiting-moderation*))))))))

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
