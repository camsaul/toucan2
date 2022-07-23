(ns toucan2.select-test
  (:require
   [clojure.test :refer :all]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.instance :as instance]
   [clojure.string :as str]
   [methodical.core :as m]
   [toucan2.model :as model])
  (:import java.time.OffsetDateTime))

(derive ::people ::test/people)

(deftest ^:parallel parse-args-test
  (doseq [[args expected] {[1]
                           [{} 1]

                           [:id 1]
                           [{:id 1} {}]

                           [{:where [:= :id 1]}]
                           [{} {:where [:= :id 1]}]

                           [:name "Cam" {:where [:= :id 1]}]
                           [{:name "Cam"} {:where [:= :id 1]}]

                           [::my-query]
                           [{} ::my-query]}]
    (testing `(select/parse-args :default ~args)
      (is (= expected
             (select/parse-args :default args))))))

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
      (is (every? #(instance/instance-of? % ::test/people)
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
             (select/select ::test/people :id [:= 1]))))))

(m/defmethod model/table-name ::people
  [_model]
  "people")

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
           (select/select ::people.name-is-pk :toucan2/pk "Cam"))))

  (testing "composite PK"
    (is (= [{:id 1, :name "Cam", :created-at (OffsetDateTime/parse "2020-04-21T23:56:00Z")}]
           (select/select ::people.composite-pk :toucan2/pk [1 "Cam"])))
    (is (= []
           (select/select ::people.composite-pk :toucan2/pk [2 "Cam"])
           (select/select ::people.composite-pk :toucan2/pk [1 "Sam"])))))

(derive ::people.no-timestamps ::people)

;; this could also be done as part a `:before` method.
(m/defmethod select/select-reducible* ::people.no-timestamps
  [model columns conditions query]
  (let [columns (or columns [:id :name])]
    (next-method model columns conditions query)))

(m/defmethod select/select-reducible* :after ::people.no-timestamps
  [_model _columns _conditions reducible-query]
  (testing "should not be an eduction yet -- if it is it means this method is getting called more than once"
    (is (not (instance? clojure.core.Eduction reducible-query))))
  (assert (not (instance? clojure.core.Eduction reducible-query)))
  (eduction
   (map (fn [person]
          (testing "select* :after should see Toucan 2 instances"
            (is (instance/instance? person)))
          (testing "instance table should be a ::people.no-timestamps"
            (is (isa? (instance/model person) ::people.no-timestamps)))
          (assoc person :after-select? true)))
   reducible-query))

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
;; to implement [[toucan2.model/build-select-query]] for `[::people.limit-2 clojure.lang.IPersistentMap]` instead. But
;; it does do a good job of letting us test that combining aux methods work like we'd expect.
(m/defmethod select/select-reducible* :before ::people.limit-2
  [_model _columns _conditions query]
  (cond-> query
    (map? query) (assoc :limit 2)))

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

#_(deftest count-test
  (is (= 4
         (select/count ::test/people)))
  (is (= 1
         (select/count ::test/people 1)))
  (is (= 3
         (select/count ::test/venues)))
  (is (= 2
         (select/count ::test/venues :category "bar"))))

#_(deftest exists?-test
  (is (= true
         (select/exists? ::test/people :name "Cam")))
  (is (= false
         (select/exists? ::test/people :name "Cam Era"))))

#_(m/defmethod honeysql.compile/to-sql* [:default :people/custom-honeysql :id String]
  [_ _ _ v _]
  (assert (string? v) (format "V should be a string, got %s" (pr-str v)))
  ["?::integer" v])

#_(deftest custom-honeysql-test
  (test/with-default-connection
    (is (= ["SELECT id, name FROM people WHERE id = ?::integer" "1"]
           (query/compiled
             (select/select :people/custom-honeysql :id "1" {:select [:id :name]}))))
    (testing "key-value condition"
      (is (= [{:id 1, :name "Cam"}]
             (select/select :people/custom-honeysql :id "1" {:select [:id :name]})))
      (testing "Toucan-style [f & args] condition"
        (is (= [{:id 1, :name "Cam"}]
               (select/select :people/custom-honeysql :id [:in ["1"]] {:select [:id :name]})))))
    (testing "as the PK"
      (testing "(single value)"
        (is (= [{:id 1, :name "Cam"}]
               (select/select :people/custom-honeysql "1" {:select [:id :name]}))))
      (testing "(vector of multiple values)"
        (is (= [{:id 1, :name "Cam"}]
               (select/select :people/custom-honeysql ["1"] {:select [:id :name]})))))))

#_(m/defmethod instance/key-transform-fn* [:default :people/custom-instance-type]
  [_ _]
  identity)

#_(m/defmethod instance/instance* [:default :people/custom-instance-type]
  [_ _ _ m _ metta]
  (with-meta m metta))

#_(m/defmethod conn.current/default-connectable-for-model* :people/custom-instance-type
  [_ _]
  :test/postgres)

#_(deftest custom-instance-type-test
  (let [m (select/select-one :people/custom-instance-type 1)]
    (is (= {:id 1, :name "Cam", :created_at (OffsetDateTime/parse "2020-04-21T23:56Z")}
           m))
    (is (map? m))
    (is (not (instance/toucan2-instance? m)))))

#_(deftest dont-add-from-if-it-already-exists-test
  (testing "Select shouldn't add a :from clause if one is passed in explicitly already"
    (is (= (instance/instance :toucan2.select-test/people {:id 1})
           (select/select-one ::people {:select [:p.id], :from [[::people :p]], :where [:= :p.id 1]})))
    (is (= ["SELECT p.id FROM people p WHERE p.id = ?" 1]
           (query/compiled
             (select/select-one ::people {:select [:p.id], :from [[::people :p]], :where [:= :p.id 1]}))))))
