(ns toucan2.tools.helpers-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
   [methodical.core :as m]
   [clojure.string :as str]
   [toucan2.insert :as insert])
  (:import java.time.LocalDateTime))

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

;; (deftest group-by-xform-test
;;   (is (= {false [2 4], true [3 5]}
;;          (into {} (toucan2.helpers/group-by-xform even? inc) [1 2 3 4])))
;;   (is (= {false [2], true [3]}
;;          (into {} (comp (take 2) (toucan2.helpers/group-by-xform even? inc)) [1 2 3 4]))))

;; (helpers/define-table-name ::venues "venues")

;; (def ^:dynamic ^:private *updated-venues* nil)

;; (helpers/define-before-update ::venues
;;   [venue]
;;   (is (instance/toucan2-instance? venue))
;;   (is (isa? (instance/tableable venue) ::venues))
;;   (when *updated-venues*
;;     (swap! *updated-venues* conj venue))
;;   venue)

;; (derive ::venues-update-updated-at ::venues)

;; (helpers/define-before-update ::venues-update-updated-at
;;   [venue]
;;   (assoc venue :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")))

;; (derive ::venues-discard-category-change ::venues)

;; (helpers/define-before-update ::venues-discard-category-change
;;   [venue]
;;   (assoc venue :category (:category (instance/original venue))))

;; (deftest before-update-test
;;   (testing "Updates returned by the before-update method should actually be applied"
;;     (testing "f adds changes"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (mutative/update! ::venues-update-updated-at 1 {:name "Kennedy's Irish Pub and Curry House"})))
;;         (is (= {:id         1
;;                 :name       "Kennedy's Irish Pub and Curry House"
;;                 :category   "bar"
;;                 :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                 :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")}
;;                (select/select-one ::venues-update-updated-at 1)))))
;;     (testing "f discards changes"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (mutative/update! ::venues-discard-category-change 1 {:name     "Kennedy's Irish Pub and Curry House"
;;                                                                      :category "bar-plus-curry-house"})))
;;         (is (= {:id         1
;;                 :name       "Kennedy's Irish Pub and Curry House"
;;                 :category   "bar"
;;                 :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                 :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
;;                (select/select-one ::venues-discard-category-change 1)))))))

;; (deftest before-update-conditions-test
;;   (testing "define-before-update should call its method with all objects that match update conditions"
;;     (testing "PK condition"
;;       (test/with-discarded-table-changes :venues
;;         (binding [*updated-venues* (atom [])]
;;           (is (= 1
;;                  (mutative/update! ::venues 1 {:name "Kennedy's Irish Pub and Curry House"})))
;;           (is (= "Kennedy's Irish Pub and Curry House"
;;                  (select/select-one-fn :name ::venues 1)))
;;           (testing "current"
;;             (is (= [(instance/instance ::venues {:id         1
;;                                                  :name       "Kennedy's Irish Pub and Curry House"
;;                                                  :category   "bar"
;;                                                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                                                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
;;                    @*updated-venues*)))
;;           (testing "original"
;;             (is (= [(instance/instance ::venues {:id         1
;;                                                  :name       "Tempest"
;;                                                  :category   "bar"
;;                                                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                                                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
;;                    (map instance/original @*updated-venues*))))
;;           (testing "changes"
;;             (is (= [{:name "Kennedy's Irish Pub and Curry House"}]
;;                    (map instance/changes @*updated-venues*)))))))

;;     (testing "non-PK condition"
;;       (test/with-discarded-table-changes :venues
;;         (binding [*updated-venues* (atom [])]
;;           (is (= 2
;;                  (mutative/update! ::venues :category [:not= "store"] {:category "not-store"})))
;;           (is (= "not-store"
;;                  (select/select-one-fn :category ::venues 1)
;;                  (select/select-one-fn :category ::venues 2)))
;;           (testing "current"
;;             (is (= [{:id         1
;;                      :name       "Tempest"
;;                      :category   "not-store"
;;                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
;;                     {:id         2
;;                      :name       "Ho's Tavern"
;;                      :category   "not-store"
;;                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
;;                    (sort-by :id @*updated-venues*))))
;;           (testing "changes"
;;             (is (= [{:category "not-store"}
;;                     {:category "not-store"}]
;;                    (map instance/changes @*updated-venues*)))))))

;;     (testing "PK + other conditions"
;;       (test/with-discarded-table-changes :venues
;;         (binding [*updated-venues* (atom [])]
;;           (is (= 1
;;                  (mutative/update! ::venues 3 :category [:not= "bar"] {:category "not-bar"})))
;;           (is (= "not-bar"
;;                  (select/select-one-fn :category ::venues 3)))
;;           (testing "current"
;;             (is (= [{:id         3
;;                      :name       "BevMo"
;;                      :category   "not-bar"
;;                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
;;                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
;;                    @*updated-venues*)))
;;           (testing "changes"
;;             (is (= [{:category "not-bar"}]
;;                    (map instance/changes @*updated-venues*)))))))))

;; (deftest before-update-no-changes-test
;;   (testing "No changes passed to update!* itself -- should no-op"
;;     (test/with-discarded-table-changes :venues
;;       (query/with-call-count [call-count]
;;         (is (zero? (mutative/update! ::venues 1 {})))
;;         (testing "Should have no calls -- no changes, no need to update anything."
;;           (is (zero? (call-count)))))))

;;   (testing "No matching instances -- should no-op"
;;     (test/with-discarded-table-changes :venues
;;       (query/with-call-count [call-count]
;;         (is (zero? (mutative/update! ::venues :category "museum" {:name "A Museum"})))
;;         (testing "Should have just one call (initial select to find matching values)"
;;           (is (= 1
;;                  (call-count)))))))

;;   (testing "No changes relative to the actual object in the DB -- should no-op"
;;     (testing "[before f]"
;;       (test/with-discarded-table-changes :venues
;;         (query/with-call-count [call-count]
;;           (is (zero? (mutative/update! ::venues 1 {:name "Tempest"})))
;;           (testing "Should have just one call (initial select to find matching values)"
;;             (is (= 1
;;                    (call-count)))))))
;;     (testing "[after f]"
;;       (test/with-discarded-table-changes :venues
;;         (query/with-call-count [call-count]
;;           (is (zero? (mutative/update! ::venues-discard-category-change 1 {:category "dive-bar"})))
;;           (testing "Should have just one call (initial select to find matching values)"
;;             (is (= 1
;;                    (call-count)))))))))

;; (def ^:private ^:dynamic *venues-update-queries* nil)

;; (derive ::venues-capture-updates ::venues)

;; (m/defmethod mutative/update!* [:default ::venues-capture-updates :default]
;;   [connectable tableable query options]
;;   (when *venues-update-queries*
;;     (swap! *venues-update-queries* conj query))
;;   (next-method connectable tableable query options))

;; (derive ::venues-add-unique-category ::venues-capture-updates)

;; (helpers/define-before-update ::venues-add-unique-category
;;   [venue]
;;   (assoc venue :category (format "category-%d" (:id venue))))

;; (deftest before-update-batch-updates-test
;;   (testing "Group together updates into one call per set of updates."
;;     (testing "Only one update needed"
;;       (test/with-discarded-table-changes :venues
;;         (query/with-call-count [call-count]
;;           (binding [*venues-update-queries* (atom [])]
;;             (is (= 2
;;                    (mutative/update! ::venues-capture-updates :category "bar" {:category "dive-bar"})))
;;             (testing "Should have 2 DB calls -- one to fetch matching rows, another to do the single update"
;;               (is (= 2
;;                      (call-count)))
;;               (testing "Don't add extra :where clauses if there's just one set of changes to apply to all matching rows."
;;                 (is (= [{:update (honeysql.compile/table-identifier ::venues-capture-updates)
;;                          :set    {:category "dive-bar"}
;;                          :where  [:= :category "bar"]}]
;;                        @*venues-update-queries*))))))))

;;     (testing "Multiple updates needed"
;;       (test/with-discarded-table-changes :venues
;;         (query/with-call-count [call-count]
;;           (binding [*venues-update-queries* (atom [])]
;;             (is (= 2
;;                    (mutative/update! ::venues-add-unique-category :category "bar"
;;                                      {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")})))
;;             (testing "Should have 3 DB calls -- one to fetch matching rows, then 2 separate updates"
;;               (is (= 3
;;                      (call-count)))
;;               (is (= [{:update (honeysql.compile/table-identifier ::venues-add-unique-category)
;;                        :set    {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
;;                                 :category   "category-1"}
;;                        :where  [:and [:= :category "bar"] [:in :id [1]]]}
;;                       {:update (honeysql.compile/table-identifier ::venues-add-unique-category)
;;                        :set    {:updated-at (LocalDateTime/parse "2021-06-09T15:18:00")
;;                                 :category   "category-2"}
;;                        :where  [:and [:= :category "bar"] [:in :id [2]]]}]
;;                      @*venues-update-queries*))))))))

;;   (testing "Limit the max number of rows we update at once."
;;     ;; TODO
;;     ))

;; (deftest before-update-transaction-test
;;   (testing "before-update should run in a transaction; an error during some part should cause all updates to be discarded"
;;     ;; TODO
;;     ))

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
  (test/with-discarded-table-changes :venues
    (binding [*venues-awaiting-moderation* (atom [])]
      (is (= 1
             (insert/insert! ::venues.after-insert {:name "Lombard Heights Market", :category "liquor-store"})))
      (is (= [(instance/instance
               ::venues.after-insert
               {:id         4
                :name       "Lombard Heights Market"
                :category   "liquor-store"
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
             @*venues-awaiting-moderation*)))))

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
