(ns toucan2.helpers-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]
            [toucan2.compile :as compile]
            [toucan2.connectable.current :as conn.current]
            [toucan2.helpers :as helpers]
            [toucan2.instance :as instance]
            [toucan2.mutative :as mutative]
            [toucan2.query :as query]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]
            [toucan2.test :as test]))

(use-fixtures :once test/do-with-test-data)

(m/defmethod tableable/table-name* [:default ::people]
  [_ _ _]
  "people")

(m/defmethod conn.current/default-connectable-for-tableable* ::people
  [_ _]
  :test/postgres)

(helpers/define-before-select ::people [query]
  (assoc query :select [:id :name]))

(helpers/define-after-select ::people [person]
  (assoc person ::after-select? true))

(deftest select-helpers-test
  (is (= (instance/instance ::people {:id                                  1
                                      :name                                "Cam"
                                      :toucan2.helpers-test/after-select? true})
         (select/select-one ::people 1))))

(deftest group-by-xform-test
  (is (= {false [2 4], true [3 5]}
         (into {} (toucan2.helpers/group-by-xform even? inc) [1 2 3 4])))
  (is (= {false [2], true [3]}
         (into {} (comp (take 2) (toucan2.helpers/group-by-xform even? inc)) [1 2 3 4]))))

(helpers/define-table-name ::venues "venues")

(m/defmethod conn.current/default-connectable-for-tableable* ::venues
  [_ _]
  :test/postgres)

(def ^:dynamic ^:private *updated-venues* nil)

(helpers/define-before-update ::venues
  [venue]
  (is (instance/toucan2-instance? venue))
  (is (isa? (instance/tableable venue) ::venues))
  (when *updated-venues*
    (swap! *updated-venues* conj venue))
  venue)

(derive ::venues-update-updated-at ::venues)

(helpers/define-before-update ::venues-update-updated-at
  [venue]
  (assoc venue :updated-at (t/local-date-time "2021-06-09T15:18:00")))

(derive ::venues-discard-category-change ::venues)

(helpers/define-before-update ::venues-discard-category-change
  [venue]
  (assoc venue :category (:category (instance/original venue))))

(deftest before-update-test
  (testing "Updates returned by the before-update method should actually be applied"
    (testing "f adds changes"
      (test/with-venues-reset
        (is (= 1
               (mutative/update! ::venues-update-updated-at 1 {:name "Kennedy's Irish Pub and Curry House"})))
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (t/local-date-time "2017-01-01T00:00")
                :updated-at (t/local-date-time "2021-06-09T15:18:00")}
               (select/select-one ::venues-update-updated-at 1)))))
    (testing "f discards changes"
      (test/with-venues-reset
        (is (= 1
               (mutative/update! ::venues-discard-category-change 1 {:name     "Kennedy's Irish Pub and Curry House"
                                                                     :category "bar-plus-curry-house"})))
        (is (= {:id         1
                :name       "Kennedy's Irish Pub and Curry House"
                :category   "bar"
                :created-at (t/local-date-time "2017-01-01T00:00")
                :updated-at (t/local-date-time "2017-01-01T00:00")}
               (select/select-one ::venues-discard-category-change 1)))))))

(deftest before-update-conditions-test
  (testing "define-before-update should call its method with all objects that match update conditions"
    (testing "PK condition"
      (test/with-venues-reset
        (binding [*updated-venues* (atom [])]
          (is (= 1
                 (mutative/update! ::venues 1 {:name "Kennedy's Irish Pub and Curry House"})))
          (is (= "Kennedy's Irish Pub and Curry House"
                 (select/select-one-fn :name ::venues 1)))
          (testing "current"
            (is (= [(instance/instance ::venues {:id         1
                                                 :name       "Kennedy's Irish Pub and Curry House"
                                                 :category   "bar"
                                                 :created-at (t/local-date-time "2017-01-01T00:00")
                                                 :updated-at (t/local-date-time "2017-01-01T00:00")})]
                   @*updated-venues*)))
          (testing "original"
            (is (= [(instance/instance ::venues {:id         1
                                                 :name       "Tempest"
                                                 :category   "bar"
                                                 :created-at (t/local-date-time "2017-01-01T00:00")
                                                 :updated-at (t/local-date-time "2017-01-01T00:00")})]
                   (map instance/original @*updated-venues*))))
          (testing "changes"
            (is (= [{:name "Kennedy's Irish Pub and Curry House"}]
                   (map instance/changes @*updated-venues*)))))))

    (testing "non-PK condition"
      (test/with-venues-reset
        (binding [*updated-venues* (atom [])]
          (is (= 2
                 (mutative/update! ::venues :category [:not= "store"] {:category "not-store"})))
          (is (= "not-store"
                 (select/select-one-fn :category ::venues 1)
                 (select/select-one-fn :category ::venues 2)))
          (testing "current"
            (is (= [{:id         1
                     :name       "Tempest"
                     :category   "not-store"
                     :created-at (t/local-date-time "2017-01-01T00:00")
                     :updated-at (t/local-date-time "2017-01-01T00:00")}
                    {:id         2
                     :name       "Ho's Tavern"
                     :category   "not-store"
                     :created-at (t/local-date-time "2017-01-01T00:00")
                     :updated-at (t/local-date-time "2017-01-01T00:00")}]
                   (sort-by :id @*updated-venues*))))
          (testing "changes"
            (is (= [{:category "not-store"}
                    {:category "not-store"}]
                   (map instance/changes @*updated-venues*)))))))

    (testing "PK + other conditions"
      (test/with-venues-reset
        (binding [*updated-venues* (atom [])]
          (is (= 1
                 (mutative/update! ::venues 3 :category [:not= "bar"] {:category "not-bar"})))
          (is (= "not-bar"
                 (select/select-one-fn :category ::venues 3)))
          (testing "current"
            (is (= [{:id         3
                     :name       "BevMo"
                     :category   "not-bar"
                     :created-at (t/local-date-time "2017-01-01T00:00")
                     :updated-at (t/local-date-time "2017-01-01T00:00")}]
                   @*updated-venues*)))
          (testing "changes"
            (is (= [{:category "not-bar"}]
                   (map instance/changes @*updated-venues*)))))))))

(deftest before-update-no-changes-test
  (testing "No changes passed to update!* itself -- should no-op"
    (test/with-venues-reset
      (query/with-call-count [call-count]
        (is (zero? (mutative/update! ::venues 1 {})))
        (testing "Should have no calls -- no changes, no need to update anything."
          (is (zero? (call-count)))))))

  (testing "No matching instances -- should no-op"
    (test/with-venues-reset
      (query/with-call-count [call-count]
        (is (zero? (mutative/update! ::venues :category "museum" {:name "A Museum"})))
        (testing "Should have just one call (initial select to find matching values)"
          (is (= 1
                 (call-count)))))))

  (testing "No changes relative to the actual object in the DB -- should no-op"
    (testing "[before f]"
      (test/with-venues-reset
        (query/with-call-count [call-count]
          (is (zero? (mutative/update! ::venues 1 {:name "Tempest"})))
          (testing "Should have just one call (initial select to find matching values)"
            (is (= 1
                   (call-count)))))))
    (testing "[after f]"
      (test/with-venues-reset
        (query/with-call-count [call-count]
          (is (zero? (mutative/update! ::venues-discard-category-change 1 {:category "dive-bar"})))
          (testing "Should have just one call (initial select to find matching values)"
            (is (= 1
                   (call-count)))))))))

(def ^:private ^:dynamic *venues-update-queries* nil)

(derive ::venues-capture-updates ::venues)

(m/defmethod mutative/update!* [:default ::venues-capture-updates :default]
  [connectable tableable query options]
  (when *venues-update-queries*
    (swap! *venues-update-queries* conj query))
  (next-method connectable tableable query options))

(derive ::venues-add-unique-category ::venues-capture-updates)

(helpers/define-before-update ::venues-add-unique-category
  [venue]
  (assoc venue :category (format "category-%d" (:id venue))))

(deftest before-update-batch-updates-test
  (testing "Group together updates into one call per set of updates."
    (testing "Only one update needed"
      (test/with-venues-reset
        (query/with-call-count [call-count]
          (binding [*venues-update-queries* (atom [])]
            (is (= 2
                   (mutative/update! ::venues-capture-updates :category "bar" {:category "dive-bar"})))
            (testing "Should have 2 DB calls -- one to fetch matching rows, another to do the single update"
              (is (= 2
                     (call-count)))
              (testing "Don't add extra :where clauses if there's just one set of changes to apply to all matching rows."
                (is (= [{:update (compile/table-identifier ::venues-capture-updates)
                         :set    {:category "dive-bar"}
                         :where  [:= :category "bar"]}]
                       @*venues-update-queries*))))))))

    (testing "Multiple updates needed"
      (test/with-venues-reset
        (query/with-call-count [call-count]
          (binding [*venues-update-queries* (atom [])]
            (is (= 2
                   (mutative/update! ::venues-add-unique-category :category "bar"
                                     {:updated-at (t/local-date-time "2021-06-09T15:18:00")})))
            (testing "Should have 3 DB calls -- one to fetch matching rows, then 2 separate updates"
              (is (= 3
                     (call-count)))
              (is (= [{:update (compile/table-identifier ::venues-add-unique-category)
                       :set    {:updated-at (t/local-date-time "2021-06-09T15:18:00")
                                :category   "category-1"}
                       :where  [:and [:= :category "bar"] [:in :id [1]]]}
                      {:update (compile/table-identifier ::venues-add-unique-category)
                       :set    {:updated-at (t/local-date-time "2021-06-09T15:18:00")
                                :category   "category-2"}
                       :where  [:and [:= :category "bar"] [:in :id [2]]]}]
                     @*venues-update-queries*))))))))

  (testing "Limit the max number of rows we update at once."
    ;; TODO
    ))

(deftest before-update-transaction-test
  (testing "before-update should run in a transaction; an error during some part should cause all updates to be discarded"
    ;; TODO
    ))

(helpers/deftransforms ::transformed-venues
  {:id {:in  #(some-> % Integer/parseInt)
        :out str}})

(helpers/define-table-name ::transformed-venues "venues")

(deftest deftransforms-test
  (is (= (instance/instance
          ::transformed-venues
          {:id         "1"
           :name       "Tempest"
           :category   "bar"
           :created-at (t/local-date-time "2017-01-01T00:00")
           :updated-at (t/local-date-time "2017-01-01T00:00")})
         (select/select-one [:test/postgres ::transformed-venues] "1"))))
