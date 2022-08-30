(ns toucan2.select-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile])
  (:import
   (java.time LocalDateTime OffsetDateTime)))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(derive ::people ::test/people)

(deftest ^:parallel parse-args-test
  (doseq [[args expected] {[:model 1]
                           {:modelable :model, :queryable 1}

                           [:model :id 1]
                           {:modelable :model, :kv-args {:id 1}, :queryable {}}

                           [:model {:where [:= :id 1]}]
                           {:modelable :model, :queryable {:where [:= :id 1]}}

                           [:model :name "Cam" {:where [:= :id 1]}]
                           {:modelable :model, :kv-args {:name "Cam"}, :queryable {:where [:= :id 1]}}

                           [:model ::my-query]
                           {:modelable :model, :queryable ::my-query}}]
    (testing `(query/parse-args :toucan.query-type/select.* ~args)
      (is (= expected
             (query/parse-args :toucan.query-type/select.* args))))))

(deftest ^:parallel default-build-query-test
  (is (= {:select [:*]
          :from   [[:default]]}
         (query/build :toucan.query-type/select.* :default {:query {}})))
  (testing "don't override existing"
    (is (= {:select [:a :b]
            :from   [[:my_table]]}
           (query/build :toucan.query-type/select.* :default {:query {:select [:a :b], :from [[:my_table]]}}))))
  (testing "columns"
    (is (= {:select [:a :b]
            :from   [[:default]]}
           (query/build :toucan.query-type/select.* :default {:query {}, :columns [:a :b]})))
    (testing "existing"
      (is (= {:select [:a]
              :from   [[:default]]}
             (query/build :toucan.query-type/select.* :default {:query {:select [:a]}, :columns [:a :b]})))))
  (testing "conditions"
    (is (= {:select [:*]
            :from   [[:default]]
            :where  [:= :id 1]}
           (query/build :toucan.query-type/select.* :default {:query {}, :kv-args {:id 1}})))
    (testing "merge with existing"
      (is (= {:select [:*]
              :from   [[:default]]
              :where  [:and [:= :a :b] [:= :id 1]]}
             (query/build :toucan.query-type/select.* :default {:query {:where [:= :a :b]}, :kv-args {:id 1}}))))))

(m/defmethod query/apply-kv-arg [:default clojure.lang.IPersistentMap ::custom.limit]
  [_model honeysql-form _k limit]
  (assoc honeysql-form :limit limit))

(deftest custom-condition-test
  (is (= {:select [:*]
          :from   [[:default]]
          :limit  100}
         (query/build :toucan.query-type/select.* :default {:query {}, :kv-args {::custom.limit 100}})
         (query/build :toucan.query-type/select.* :default {:query {:limit 1}, :kv-args {::custom.limit 100}}))))

(deftest built-in-pk-condition-test
  (is (= {:select [:*], :from [[:default]], :where [:= :id 1]}
         (query/build :toucan.query-type/select.* :default {:query {}, :kv-args {:toucan/pk 1}})))
  (is (= {:select [:*], :from [[:default]], :where [:and
                                                    [:= :name "Cam"]
                                                    [:= :id 1]]}
         (query/build :toucan.query-type/select.* :default {:query {:where [:= :name "Cam"]}, :kv-args {:toucan/pk 1}}))))

(deftest select-test
  (let [expected [(instance/instance ::test/people {:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})]]
    (testing "plain SQL"
      (is (= expected
             (select/select ::test/people "SELECT * FROM people WHERE id = 1;"))))
    (testing "sql-args"
      (is (= expected
             (select/select ::test/people ["SELECT * FROM people WHERE id = ?;" 1]))))
    (testing "HoneySQL"
      (is (= expected
             (select/select ::test/people {:select [:*], :from [[:people]], :where [:= :id 1]}))))
    (testing "PK"
      (is (= expected
             (select/select ::test/people 1))))
    (testing "conditions"
      (is (= expected
             (select/select ::test/people :id 1))))
    (testing "columns"
      (is (= [(instance/instance ::test/people {:id 1})]
             (select/select [::test/people :id] :id 1))))))

(deftest select-test-2
  (let [all-rows [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}
                  {:id 2, :name "Sam", :created-at (OffsetDateTime/parse "2019-01-11T23:56:00Z")}
                  {:id 3, :name "Pam", :created-at (OffsetDateTime/parse "2020-01-01T21:56:00Z")}
                  {:id 4, :name "Tam", :created-at (OffsetDateTime/parse "2020-05-25T19:56:00Z")}]]
    (testing "no args"
      (is (= all-rows
             (sort-by :id (select/select ::people))))
      (is (= all-rows
             (sort-by :id (select/select ::test/people))))
      (is (every? (partial instance/instance-of? ::test/people)
                  (select/select ::test/people {:order-by [[:id :asc]]})))))
  (testing "one arg (id)"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::people 1))))
  (testing "one arg (query)"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::test/people {:where [:= :id 1]})))
    (is (= [{:id 1, :name "Tempest", :category "bar"}]
           (select/select ::test/venues {:select [:id :name :category], :limit 1, :where [:= :id 1]}))))
  (testing "two args (k v)"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::test/people :id 1)))
    (testing "sequential v"
      (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
             (select/select ::test/people :id [:= 1])))))
  (testing "k v conditions + query"
    (is (= [(instance/instance ::test/venues {:id 3, :name "BevMo"})]
           (select/select ::test/venues :id [:>= 3] {:select [:id :name], :order-by [[:id :asc]]})))))

(m/defmethod query/do-with-resolved-query [:default ::count-query]
  [model _queryable f]
  (query/do-with-resolved-query model
                                {:select [[:%count.* :count]]
                                 :from   [(keyword (model/table-name model))]}
                                f))

(deftest named-query-test
  (testing "venues"
    (is (= [{:count 3}]
           (select/select ::test/venues ::count-query))))
  (testing "people"
    (is (= [{:count 4}]
           (select/select ::test/people ::count-query))))
  (testing "with additional conditions"
    (is (= [{:count 1}]
           (select/select ::test/venues :id 1 ::count-query)))))

(derive ::people.name-is-pk ::people)

(m/defmethod model/primary-keys ::people.name-is-pk
  [_model]
  :name)

(derive ::people.composite-pk ::people)

(m/defmethod model/primary-keys ::people.composite-pk
  [_model]
  [:id :name])

(deftest select-non-integer-pks-test
  (testing "non-integer PK"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::people.name-is-pk :toucan/pk "Cam"))))

  (testing "composite PK"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::people.composite-pk :toucan/pk [1 "Cam"])))
    (is (= []
           (select/select ::people.composite-pk :toucan/pk [2 "Cam"])
           (select/select ::people.composite-pk :toucan/pk [1 "Sam"])))))

(derive ::people.no-timestamps ::people)

(m/defmethod pipeline/transduce-with-model* :before [:toucan.query-type/select.* ::people.no-timestamps]
  [_rf _query-type _model parsed-args]
  (update parsed-args :columns (fn [columns]
                                 (or columns [:id :name]))))

(m/defmethod pipeline/transduce-with-model* [:toucan.query-type/select.instances ::people.no-timestamps]
  [rf query-type model parsed-args]
  (let [rf* ((map (fn [person]
                    (testing "select* :after should see Toucan 2 instances"
                      (is (instance/instance? person)))
                    (testing "instance table should be a ::people.no-timestamps"
                      (is (isa? (protocols/model person) ::people.no-timestamps)))
                    (assoc person :after-select? true)))
             rf)]
    (next-method rf* query-type model parsed-args)))

(deftest default-query-test
  (testing "Should be able to set some defaults by implementing `select*`"
    (is (= [(instance/instance ::people.no-timestamps {:id 1, :name "Cam", :after-select? true})]
           (select/select ::people.no-timestamps 1)))))

(deftest post-select-test
  (testing "Should be able to do cool stuff in (select* :after)"
    (testing (str \newline '(ancestors ::people.no-timestamps) " => " (pr-str (ancestors ::people.no-timestamps)))
      (is (= [(instance/instance ::people.no-timestamps {:id 1, :name "Cam", :after-select? true})
              (instance/instance ::people.no-timestamps {:id 2, :name "Sam", :after-select? true})
              (instance/instance ::people.no-timestamps {:id 3, :name "Pam", :after-select? true})
              (instance/instance ::people.no-timestamps {:id 4, :name "Tam", :after-select? true})]
             (select/select ::people.no-timestamps))))))

(derive ::people.limit-2 ::people)

;; TODO this is probably not the way you'd want to accomplish this in real life -- I think you'd probably actually want
;; to implement [[toucan2.query/build]] instead. But it does do a good job of letting us test that combining aux methods
;; work like we'd expect.
(m/defmethod pipeline/transduce-built-query* :before [:toucan.query-type/select.* ::people.limit-2 clojure.lang.IPersistentMap]
  [_rf _query-type _model built-query]
  (assoc built-query :limit 2))

(deftest pre-select-test
  (testing "Should be able to do cool stuff in pre-select (select* :before)"
    (is (= [(instance/instance ::people.limit-2 {:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})
            (instance/instance ::people.limit-2 {:id 2, :name "Sam", :created-at (OffsetDateTime/parse "2019-01-11T23:56Z")})]
           (select/select ::people.limit-2)))))

(derive ::people.no-timestamps-limit-2 ::people.no-timestamps)
(derive ::people.no-timestamps-limit-2 ::people.limit-2)

(deftest combine-aux-methods-test
  (testing (str \newline '(ancestors ::people.no-timestamps-limit-2) " => " (pr-str (ancestors ::people.no-timestamps-limit-2)))
    (is (= [(instance/instance ::people.no-timestamps-limit-2 {:id 1, :name "Cam", :after-select? true})
            (instance/instance ::people.no-timestamps-limit-2 {:id 2, :name "Sam", :after-select? true})]
           (select/select ::people.no-timestamps-limit-2)))))

(deftest select-one-test
  (is (= (instance/instance ::test/people {:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})
         (select/select-one ::test/people 1)))
  (is (= nil
         (select/select-one ::test/people :id 1000))))

;;; TODO -- a test to make sure this doesn't fetch a second row even if query would return multiple rows. A test with a
;;; SQL query.

(deftest select-fn-test
  (testing "Equivalent of Toucan select-field"
    (is (= #{1 2 3 4}
           (select/select-fn-set :id ::test/people))))
  (testing "Return vector instead of a set"
    (is (= [1 2 3 4]
           (select/select-fn-vec :id ::test/people))))
  (testing "Arbitrary function instead of a key"
    (is (= [2 3 4 5]
           (select/select-fn-vec (comp inc :id) ::test/people))))
  (testing "Should work with magical keys"
    (doseq [k [:created-at :created_at]]
      (testing k
        (is (= [(OffsetDateTime/parse "2020-04-21T23:56Z")
                (OffsetDateTime/parse "2019-01-11T23:56Z")
                (OffsetDateTime/parse "2020-01-01T21:56Z")
                (OffsetDateTime/parse "2020-05-25T19:56Z")]
               (select/select-fn-vec k ::test/people {:order-by [[:id :asc]]}))))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-fn-set :id ::test/people :id 100)))
    (is (nil? (select/select-fn-vec :id ::test/people :id 100)))))

(deftest select-one-fn-test
  (is (= 1
         (select/select-one-fn :id ::test/people :name "Cam"))))

(deftest select-pks-test
  (is (= #{1 2 3 4}
         (select/select-pks-set ::test/people)))
  (is (= [1 2 3 4]
         (select/select-pks-vec ::test/people)))
  (testing "non-integer PK"
    (is (= #{"Cam" "Sam" "Pam" "Tam"}
           (select/select-pks-set ::people.name-is-pk)))
    (is (= ["Cam" "Sam" "Pam" "Tam"]
           (select/select-pks-vec ::people.name-is-pk))))
  (testing "Composite PK -- should return vectors"
    (is (= #{[1 "Cam"] [2 "Sam"] [3 "Pam"] [4 "Tam"]}
           (select/select-pks-set ::people.composite-pk)))
    (is (= [[1 "Cam"] [2 "Sam"] [3 "Pam"] [4 "Tam"]]
           (select/select-pks-vec ::people.composite-pk))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-pks-set ::test/people :id 100)))
    (is (nil? (select/select-pks-vec ::test/people :id 100)))))

(deftest select-one-pk-test
  (is (= 1
         (select/select-one-pk ::test/people :name "Cam")))
  (testing "non-integer PK"
    (is (= "Cam"
           (select/select-one-pk ::people.name-is-pk :id 1))))
  (testing "Composite PK -- should return vector"
    (is (= [1 "Cam"]
           (select/select-one-pk ::people.composite-pk :id 1)))))

(deftest select-fn->fn-test
  (is (= {1 "Cam", 2 "Sam", 3 "Pam", 4 "Tam"}
         (select/select-fn->fn :id :name ::test/people)))
  (is (= {2 "cam", 3 "sam", 4 "pam", 5 "tam"}
         (select/select-fn->fn (comp inc :id) (comp str/lower-case :name) ::test/people)))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-fn->fn :id :name ::test/people :id 100)))))

(deftest select-pk->fn-test
  (is (= {1 "Cam", 2 "Sam", 3 "Pam", 4 "Tam"}
         (select/select-pk->fn :name ::test/people)))
  (is (= {1 "cam", 2 "sam", 3 "pam", 4 "tam"}
         (select/select-pk->fn (comp str/lower-case :name) ::test/people)))
  (testing "Composite PKs"
    (is (= {[1 "Cam"] "Cam", [2 "Sam"] "Sam", [3 "Pam"] "Pam", [4 "Tam"] "Tam"}
           (select/select-pk->fn :name ::people.composite-pk))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-pk->fn :name ::test/people :id 100)))))

(deftest select-fn->pk-test
  (is (= {"Cam" 1, "Sam" 2, "Pam" 3, "Tam" 4}
         (select/select-fn->pk :name ::test/people)))
  (is (= {"cam" 1, "sam" 2, "pam" 3, "tam" 4}
         (select/select-fn->pk (comp str/lower-case :name) ::test/people)))
  (testing "Composite PKs"
    (is (= {"Cam" [1 "Cam"], "Sam" [2 "Sam"], "Pam" [3 "Pam"], "Tam" [4 "Tam"]}
  (testing "Should return nil if the result is empty"
           (select/select-fn->pk :name ::people.composite-pk))))
    (is (nil? (select/select-fn->pk :name ::people.composite-pk :id 100)))))

(deftest count-test
  (is (= 4
         (select/count ::test/people)))
  (is (= 1
         (select/count ::test/people 1)))
  (is (= 3
         (select/count ::test/venues)))
  (is (= 2
         (select/count ::test/venues :category "bar"))))

(deftest exists?-test
  (is (= true
         (select/exists? ::test/people :name "Cam")))
  (is (= false
         (select/exists? ::test/people :name "Cam Era"))))

#_(derive ::people.custom-instance-type ::test/people)

#_(m/defmethod instance/key-transform-fn* [:default ::people.custom-instance-type]
    [_ _]
    identity)

#_(m/defmethod instance/instance* [:default ::people.custom-instance-type]
  [_ _ _ m _ metta]
  (with-meta m metta))

#_(deftest custom-instance-type-test
  (let [m (select/select-one ::people.custom-instance-type 1)]
    (is (= {:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")}
           m))
    (is (map? m))
    (is (not (instance/toucan2-instance? m)))))

(deftest dont-add-from-if-it-already-exists-test
  (testing "Select shouldn't add a :from clause if one is passed in explicitly already"
    (is (= (instance/instance ::test/people {:id 1})
           (select/select-one ::test/people {:select [:p.id], :from [[:people :p]], :where [:= :p.id 1]})))
    (is (= ["SELECT p.id FROM people AS p WHERE p.id = ?" 1]
           (tools.compile/compile
             (select/select-one :people {:select [:p.id], :from [[:people :p]], :where [:= :p.id 1]}))))))

(deftest select-nil-test
  (testing "(select model nil) should basically be the same as (select model :toucan/pk nil)"
    (let [parsed-args (query/parse-args :toucan.query-type/select.* [::test/venues nil])]
      (is (= {:modelable ::test/venues, :queryable nil}
             parsed-args))
      (query/with-resolved-query [query [::test/venues (:queryable parsed-args)]]
        (is (= nil
               query))
        (is (= {:select [:*], :from [[:venues]], :where [:= :id nil]}
               (query/build :toucan.query-type/select.* ::test/venues (assoc parsed-args :query query))))))
    (is (= ["SELECT * FROM venues WHERE id IS NULL"]
           (tools.compile/compile
             (select/select ::test/venues nil))))
    (is (= []
           (select/select ::test/venues nil)))
    (is (= nil
           (select/select-one ::test/venues nil)
           (select/select-one-fn :id ::test/venues nil)
           (select/select-one-fn int ::test/venues nil)))))

(deftest select-join-test
  (testing "Extra columns from joined tables should come back"
    (is (= (instance/instance ::test/venues
                              {:id              1
                               :name            "bar"
                               :category        "bar"
                               :created-at      (LocalDateTime/parse "2017-01-01T00:00")
                               :updated-at      (LocalDateTime/parse "2017-01-01T00:00")
                               :slug            "bar_01"
                               :parent-category nil})
           (select/select-one ::test/venues
                              {:left-join [[:category :c] [:= :venues.category :c.name]]
                               :order-by  [[:id :asc]]})))))

(derive ::venues.with-category ::test/venues)

(m/defmethod query/build :after [#_query-type :toucan.query-type/select.*
                                 #_model      ::venues.with-category
                                 #_query      clojure.lang.IPersistentMap]
  [_query-type _model built-query]
  (assoc built-query :left-join [[:category :c] [:= :venues.category :c.name]]))

(deftest joined-model-test
  (is (= (instance/instance ::venues.with-category
                            {:id              1
                             :name            "bar"
                             :category        "bar"
                             :created-at      (LocalDateTime/parse "2017-01-01T00:00")
                             :updated-at      (LocalDateTime/parse "2017-01-01T00:00")
                             :slug            "bar_01"
                             :parent-category nil})
         (select/select-one ::venues.with-category
                            {:left-join [[:category :c] [:= :venues.category :c.name]]
                             :order-by  [[:id :asc]]}))))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::venues.namespaced :venue})

(deftest namespaced-test
  (is (= {"venues" :venue}
         (model/table-name->namespace ::venues.namespaced)))
  (testing "When selecting a model with a namespace, keys should come back in that namespace."
    (is (= (instance/instance ::venues.namespaced
                              {:venue/id         1
                               :venue/name       "Tempest"
                               :venue/category   "bar"
                               :venue/created-at (LocalDateTime/parse "2017-01-01T00:00")
                               :venue/updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           (select/select-one ::venues.namespaced {:order-by [[:id :asc]]})))))

(doto ::venues.namespaced.with-category
  (derive ::venues.namespaced)
  (derive ::venues.with-category))

(m/defmethod model/model->namespace ::venues.namespaced.with-category
  [_model]
  {::test/venues     :venue
   ::test/categories :category})

(deftest namespaced-with-joins-test
  (is (= (toucan2.instance/instance
          ::venues.namespaced.with-category
          {:venue/id                 1
           :venue/name               "Tempest"
           :venue/category           "bar"
           :venue/created-at         (LocalDateTime/parse "2017-01-01T00:00")
           :venue/updated-at         (LocalDateTime/parse "2017-01-01T00:00")
           :category/name            "bar"
           :category/slug            "bar_01"
           :category/parent-category nil})
         (select/select-one ::venues.namespaced.with-category {:order-by [[:id :asc]]}))))
