(ns toucan2.tools.before-update-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.tools.named-query :as named-query]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime OffsetDateTime)))

(set! *warn-on-reflection* true)

(def ^:dynamic ^:private *updated-venues* nil)

(derive ::venues.before-update ::test.track-realized/venues)

(before-update/define-before-update ::venues.before-update
  [venue]
  (assert (map? venue) (format "Expected venue to be a map, got ^%s %s" (some-> venue class .getCanonicalName) (pr-str venue)))
  (is (isa? (protocols/model venue) ::venues.before-update))
  (when *updated-venues*
    (swap! *updated-venues* conj venue))
  venue)

(derive ::venues.update-updated-at ::venues.before-update)

(before-update/define-before-update ::venues.update-updated-at
  [venue]
  (assert (map? venue) (format "Expected venue to be a map, got ^%s %s" (some-> venue class .getCanonicalName) (pr-str venue)))
  (is (isa? (protocols/model venue) ::venues.before-update))
  (assoc venue :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")))

(deftest ^:synchronized add-changes-test
  (testing "Updates returned by the before-update method should actually be applied"
    (testing "method adds changes"
      (test/with-discarded-table-changes :venues
        (test.track-realized/with-realized-columns [_realized-columns]
          (is (= 1
                 (update/update! ::venues.update-updated-at 1 {:name "Kennedy's Irish Pub and Curry House"}))
              "number of rows updated")
          ;; disabled for now. We currently realize the entire row before doing before-update stuff... here we clearly
          ;; could have got away without doing it but it means we have to be super careful everywhere else to make sure
          ;; things work the way we want.
          #_(testing "\nOnly name (which is the column updated here), and ID should be realized"
              ;; It seems like `updated_at` should maybe get realized too so we can see if it changed, but it currently
              ;; does not; that's probably fine for now tho. If that changes in the future we can update this test without
              ;; worrying to much tho.
              (is (= #{:venues/name :venues/id}
                     (realized-columns)))))
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2021-06-09T15:18:00")}
               (select/select-one ::venues.update-updated-at 1)))))))

(derive ::venues.with-removed-primary-key ::venues.before-update)

(before-update/define-before-update ::venues.with-removed-primary-key
  [venue]
  (dissoc venue :id))

(deftest ^:synchronized missing-id-test
  (testing "Removing the ID in the `before-update` method isn't catastrophic"
    (test/with-discarded-table-changes :venues
      (test.track-realized/with-realized-columns [_realized-columns]
        ;; The bad case tested here only happened in very precise conditions:
        ;; - your `updated-at` method's result did not have a primary key
        ;; - your "where" matches multiple rows
        ;; - *part* of your update will change all the matched rows
        ;; - another part of your update will change a subset of matched rows
        ;;
        ;; In this test:
        ;; - we're matching rows 1 and 3
        ;; - `:updated-at` will change for both rows 1 and 3
        ;; - `:category` will only change for row 1 (3 is already a "store")
        (let [row-2 {:id 2 :name "Ho's Tavern" :category "bar"}]
          (testing "sanity check"
            (is (= row-2 (select/select-one [::venues.with-removed-primary-key :id :name :category] :id 2))))
          (testing "we only update 2 rows"
            (is (= 2 (update/update! ::venues.with-removed-primary-key :id [:in [1 3]] {:category "store" :updated-at (LocalDateTime/parse "2024-11-22T00:00")}))))
          (testing "the other row is unchanged"
            (is (= row-2 (select/select-one [::venues.with-removed-primary-key :id :name :category] :id 2)))))))))

(derive ::venues.discard-category-change ::venues.before-update)

(before-update/define-before-update ::venues.discard-category-change
  [venue]
  (assert (map? venue) (format "Expected venue to be a map, got ^%s %s" (some-> venue class .getCanonicalName) (pr-str venue)))
  (is (isa? (protocols/model venue) ::venues.before-update))
  (assoc venue :category (:category (protocols/original venue))))

(deftest ^:synchronized discard-changes-test
  (testing "Updates returned by the before-update method should actually be applied"
    (testing "method discards changes"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! ::venues.discard-category-change 1 {:name     "Kennedy's Irish Pub and Curry House"
                                                                   :category "bar-plus-curry-house"}))
            "number of rows updated")
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
               (select/select-one ::venues.discard-category-change 1)))))))

(deftest ^:synchronized pk-condition-test
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
                   (map protocols/original @*updated-venues*))))
          (testing "changes"
            (is (= [{:name "Kennedy's Irish Pub and Curry House"}]
                   (map protocols/changes @*updated-venues*)))))))))

(deftest ^:synchronized non-pk-condition-test
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
                   (map protocols/changes @*updated-venues*)))))))))

(deftest ^:synchronized pk-and-other-conditons-test
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
                   (map protocols/changes @*updated-venues*)))))))))

(deftest ^:synchronized conditions-map-test
  (testing "Should work with conditions maps"
    (test/with-discarded-table-changes :venues
      (binding [*updated-venues* (atom [])]
        (is (= 1
               (update/update! ::venues.before-update {:id 3, :category [:not= "bar"]} {:category "not-bar"})))
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
                 (map protocols/changes @*updated-venues*))))))))

(deftest ^:synchronized no-changes-test
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

(m/defmethod pipeline/compile [#_query-type  :toucan.query-type/update.*
                               #_model       ::venues.capture-updates
                               #_built-query clojure.lang.IPersistentMap]
  [query-type model built-query]
  (when *venues-update-queries*
    (swap! *venues-update-queries* conj built-query))
  (next-method query-type model built-query))

;;; this changes the category to `category-<id>` with the venue ID
(derive ::venues.add-unique-category ::venues.capture-updates)

(m/prefer-method! #'before-update/before-update ::venues.add-unique-category ::venues.before-update)

(before-update/define-before-update ::venues.add-unique-category
  [venue]
  (assoc venue :category (format "category-%d" (:id venue))))

(deftest ^:synchronized batch-updates-one-batch-test
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
                (is (= [{:update [:venues]
                         :set    {:category "dive-bar"}
                         :where  [:= :category "bar"]}]
                       @*venues-update-queries*))))))))))

(deftest ^:synchronized batch-updates-multiple-batches-test
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
              (is (= [{:update [:venues]
                       :set    {:updated-at (LocalDateTime/parse "2021-06-09T15:18")
                                :category   "category-1"}
                       :where  [:= :id 1]}
                      {:update [:venues]
                       :set    {:updated-at (LocalDateTime/parse "2021-06-09T15:18")
                                :category   "category-2"}
                       :where  [:= :id 2]}]
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

(derive ::venues.upper-case-name ::test/venues)

(before-update/define-before-update ::venues.upper-case-name
  [venue]
  (update venue :name str/upper-case))

(derive ::venues.upper-case-name.unique-category ::venues.upper-case-name)
(derive ::venues.upper-case-name.unique-category ::venues.add-unique-category)

(m/prefer-method! #'before-update/before-update ::venues.upper-case-name ::venues.before-update)
(m/prefer-method! #'before-update/before-update ::venues.add-unique-category ::venues.upper-case-name)

(deftest ^:synchronized should-compose-test
  (testing "Make sure define-before-update composes"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (update/update! ::venues.upper-case-name.unique-category :id 1
                             {:updated-at (LocalDateTime/parse "2022-08-22T18:17:00")})))
      (is (= {:id 1, :name "TEMPEST", :category "category-1"}
             (select/select-one [::test/venues :id :name :category] 1))))))

(derive ::venues.disallow-stores ::test/venues)

(before-update/define-before-update ::venues.disallow-stores
  [venue]
  (assert (map? venue) (format "Expected venue to be a map, got ^%s %s" (some-> venue class .getCanonicalName) (pr-str venue)))
  (assert (:category venue))
  (cond-> venue
    (= (:category venue) "store")
    (assoc :category nil)))

(derive ::venues.upper-case-name.no-stores ::venues.upper-case-name)
(derive ::venues.upper-case-name.no-stores ::venues.disallow-stores)

(m/prefer-method! #'before-update/before-update ::venues.upper-case-name ::venues.disallow-stores)

(deftest ^:synchronized transaction-test
  (testing "before-update should run in a transaction; an error during some part should cause all updates to be discarded"
    (testing "Do something that will have to do one updates for each row"
      (test/with-discarded-table-changes :venues
        (is (= ::venues.upper-case-name
               (:dispatch-value (meta (m/effective-method before-update/before-update ::venues.upper-case-name)))))
        (is (= 3
               (update/update! ::venues.upper-case-name {:updated-at (LocalDateTime/parse "2022-08-22T18:17:00")})))
        (is (= [{:id 1, :name "TEMPEST"}
                {:id 2, :name "HO'S TAVERN"}
                {:id 3, :name "BEVMO"}]
               (select/select [::venues.upper-case-name :id :name] {:order-by [[:id :asc]]})))))
    (test/with-discarded-table-changes :venues
      (is (thrown-with-msg?
           Throwable
           (case (test/current-db-type)
             :postgres #"null value in column \"category\" of relation \"venues\" violates not-null constraint"
             :h2       #"NULL not allowed for column \"CATEGORY\";"
             :mariadb  #"Column 'category' cannot be null")
           (update/update! ::venues.upper-case-name.no-stores {:updated-at (LocalDateTime/parse "2022-08-22T18:17:00")})))
      (is (= [{:id 1, :name "Tempest", :category "bar"}
              {:id 2, :name "Ho's Tavern", :category "bar"}
              {:id 3, :name "BevMo", :category "store"}]
             (select/select [::venues.upper-case-name.no-stores :id :name :category] {:order-by [[:id :asc]]}))))))

(derive ::venues.short-name ::test/venues)

(after-select/define-after-select ::venues.short-name
  [venue]
  (assoc venue :short-name (str/join (take 4 (:name venue)))))

(before-update/define-before-update ::venues.short-name
  [venue]
  (update venue :name str/upper-case))

(deftest ^:synchronized ignore-columns-added-by-after-select-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::venues.short-name 3 {:name "BevLess"})))))

(deftest ^:parallel preserve-model-test
  (testing "If before-update is called with an instance of a different model, preserve its model"
    (doseq [m    [{}
                  (instance/instance ::venues.short-name)
                  (instance/instance ::some-other-model)]
            :let [m        (assoc m :name "BevLess")
                  expected (assoc m :name "BEVLESS")
                  actual   (before-update/before-update ::venues.short-name m)]]
      (is (= expected
             actual))
      (is (= (protocols/model expected)
             (protocols/model actual))))))

(derive ::people.suffix-name ::test/people)

(def ^:private ^:dynamic *updated-people* nil)

(before-update/define-before-update ::people.suffix-name
  [person]
  (when *updated-people*
    (swap! *updated-people* conj (:id person)))
  (cond-> person
    (:name person) (update :name #(str % " 2.0"))))

(deftest ^:synchronized only-call-once-test
  (test/with-discarded-table-changes :people
    (testing "before-update method should be applied exactly once"
      (binding [*updated-people* (atom [])]
        (is (= 1
               (update/update! ::people.suffix-name 1 {:name "CAM"})))
        (is (= [1]
               @*updated-people*))
        (is (= {:id 1, :name "CAM 2.0"}
               (select/select-one [::people.suffix-name :id :name] 1)))))))

(derive ::people.before-update-returns-map ::test/people)

(before-update/define-before-update ::people.before-update-returns-map
  [person]
  (merge {:name "Cam 2.0"} person))

(deftest ^:synchronized before-update-that-returns-plain-map-test
  (testing "if a before-update method returns a plain map, use those as the changes"
    (test/with-discarded-table-changes :people
      (is (= 1
             (update/update! ::people.before-update-returns-map 1 {:name "Cam v2"})))
      (is (= {:id 1, :name "Cam v2"}
             (select/select-one [::people.before-update-returns-map :id :name] 1))))))

(derive ::venues.discard-category-change.default-fields ::venues.discard-category-change)

(default-fields/define-default-fields ::venues.discard-category-change.default-fields
  [:id :category])

(deftest ^:synchronized before-update-with-default-fields-test
  (testing "should be able to update fields that don't appear in default-fields"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (update/update! ::venues.discard-category-change.default-fields 1 {:name "Chase Center", :category "stadium"})))
      (is (= {:id 1, :name "Chase Center", :category "bar"}
             (select/select-one [::test/venues :id :name :category] 1))))))

(named-query/define-named-query ::people.named-conditions
  {:id [:> 1]})

(deftest ^:synchronized named-query-test
  (doseq [update! [#'update/update!
                   #'update/update-returning-pks!]]
    (test/with-discarded-table-changes :people
      (testing update!
        (is (= (condp = update!
                 #'update/update!               3
                 #'update/update-returning-pks! [2 3 4])
               (update! ::people.suffix-name ::people.named-conditions {:name "CAM"})))
        (is (= {:id         2
                :name       "CAM 2.0"
                :created-at (OffsetDateTime/parse "2019-01-11T23:56Z")}
               (select/select-one ::test/people 2)))))))

(deftest ^:parallel macroexpansion-test
  (testing "define-before-update should define vars with different names based on the model."
    (letfn [(generated-name* [form]
              (cond
                (sequential? form)
                (some generated-name* form)

                (and (symbol? form)
                     (str/starts-with? (name form) "before-update")) form))
            (generated-name [form]
              (let [expanded (walk/macroexpand-all form)]
                (or (generated-name* expanded)
                    ['no-match expanded])))]
      (is (= 'before-update-primary-method-model-1
             (generated-name `(before-update/define-before-update :model-1
                                [~'venue]
                                ~'venue))))
      (is (= 'before-update-primary-method-model-2
             (generated-name `(before-update/define-before-update :model-2
                                [~'venue]
                                ~'venue)))))))
