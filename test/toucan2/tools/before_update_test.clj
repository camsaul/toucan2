(ns toucan2.tools.before-update-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.helpers :as helpers]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(use-fixtures :each test/do-db-types-fixture)

(def ^:dynamic ^:private *updated-venues* nil)

(derive ::venues.before-update ::test/venues)

(before-update/define-before-update ::venues.before-update
  [venue]
  (is (instance/instance? venue))
  (is (isa? (instance/model venue) ::venues.before-update))
  (when *updated-venues*
    (swap! *updated-venues* conj venue))
  venue)

(derive ::venues.update-updated-at ::venues.before-update)

(before-update/define-before-update ::venues.update-updated-at
  [venue]
  (assoc venue :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")))

(derive ::venues.discard-category-change ::venues.before-update)

(before-update/define-before-update ::venues.discard-category-change
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

(before-update/define-before-update ::venues.add-unique-category
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
                (is (= [{:kv-args                             {:category "bar"}
                         :changes                             {:category "dive-bar"}
                         :query                               {}
                         ::before-update/doing-before-update? true}]
                       @*venues-update-queries*))))))))))

(deftest before-update-batch-updates-multiple-batches-test
  (testing "Group together updates into one call per set of updates."
    (testing "Multiple updates needed"
      (test/with-discarded-table-changes :venues
        (execute/with-call-count [call-count]
          (binding [*venues-update-queries* (atom [])]
            (testing "Should affect two rows"
              (is (= 2
                     (update/update! ::venues.add-unique-category :category "bar"
                                     {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")}))))
            (testing "Should have 3 DB calls -- one to fetch matching rows, then 2 separate updates"
              (is (= 3
                     (call-count)))
              (is (= [{:changes                             {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
                                                             :category   "category-1"}
                       :query                               {}
                       :kv-args                             {:id 1}
                       ::before-update/doing-before-update? true}
                      {:changes                             {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
                                                             :category   "category-2"}
                       :query                               {}
                       :kv-args                             {:id 2}
                       ::before-update/doing-before-update? true}]
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

(derive ::venues.short-name ::test/venues)

(helpers/define-after-select-each ::venues.short-name
  [venue]
  (assoc venue :short-name (str/join (take 4 (:name venue)))))

(before-update/define-before-update ::venues.short-name
  [venue]
  (update venue :name str/upper-case))

(deftest before-update-ignore-columns-added-by-after-select-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::venues.short-name 3 {:name "BevLess"})))))
