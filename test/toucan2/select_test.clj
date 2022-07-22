(ns toucan2.select-test
  (:require
   [clojure.test :refer :all]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.instance :as instance]))

(deftest ^:parallel parse-select-args-test
  (doseq [[args expected] {[1]
                           {:query 1}

                           [:id 1]
                           {:conditions {:id 1}, :query {}}

                           [{:where [:= :id 1]}]
                           {:query {:where [:= :id 1]}}

                           [:name "Cam" {:where [:= :id 1]}]
                           {:conditions {:name "Cam"}, :query {:where [:= :id 1]}}

                           [::my-query]
                           {:query ::my-query}}]
    (testing `(select/parse-select-args ~args)
      (is (= expected
             (select/parse-select-args args))))))

(deftest select-test
  (let [expected [(instance/instance ::test/people {:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")})]]
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

#_(deftest select-test
  (let [all-rows [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}
                  {:id 2, :name "Sam", :created-at (t/offset-date-time "2019-01-11T23:56:00Z")}
                  {:id 3, :name "Pam", :created-at (t/offset-date-time "2020-01-01T21:56:00Z")}
                  {:id 4, :name "Tam", :created-at (t/offset-date-time "2020-05-25T19:56:00Z")}]]
    (testing "no args"
      (is (= all-rows
             (sort-by :id (select/select ::people))))
      (is (= all-rows
             (sort-by :id (select/select ::test/people))))
      (test-people-instances? (select/select ::test/people {:order-by [[:id :asc]]})))
    (testing "using current connection (dynamic binding)"
      (binding [conn.current/*current-connectable* :test/postgres]
        (is (= all-rows
               (select/select :people {:order-by [[:id :asc]]})))))
    (testing "using default connection"
      (test/with-default-connection
        (is (= all-rows
               (select/select :people {:order-by [[:id :asc]]})))))
    (testing "with default connectable for tableable"
      (is (= all-rows
             (select/select ::people {:order-by [[:id :asc]]})))))

  (testing "one arg (id)"
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (select/select ::people 1))))

  (testing "one arg (query)"
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (select/select ::test/people {:where [:= :id 1]})))
    (is (= [{:id 1, :name "Tempest", :category "bar"}]
           (select/select [:test/postgres :venues] {:select [:id :name :category], :limit 1, :where [:= :id 1]}))))

  (testing "two args (k v)"
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (select/select ::test/people :id 1)))
    (testing "sequential v"
      (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
             (select/select ::test/people :id [:= 1]))))))

#_(m/defmethod tableable/primary-key* [:default :people/name-is-pk]
  [_ _]
  :name)

#_(m/defmethod tableable/primary-key* [:default :people/composite-pk]
  [_ _]
  [:id :name])

#_(deftest select-non-integer-pks-test
  (testing "non-integer PK"
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (select/select [:test/postgres :people/name-is-pk] "Cam"))))

  (testing "composite PK"
    (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (select/select [:test/postgres :people/composite-pk] [1 "Cam"])))
    (is (= []
           (select/select [:test/postgres :people/composite-pk] [2 "Cam"])
           (select/select [:test/postgres :people/composite-pk] [1 "Sam"])))))

;; this could also be done as part a `:before` method.
#_(m/defmethod select/select* [:default :people/no-timestamps :toucan2/honeysql]
  [connectable tableable query options]
  (let [query (assoc query :select [:id :name])]
    (next-method connectable tableable query options)))

#_(deftest default-query-test
  (testing "Should be able to set some defaults by implementing `select*`"
    (test/with-default-connection
      (is (= [(instance/instance :people/no-timestamps {:id 1, :name "Cam"})]
             (select/select :people/no-timestamps 1))))))

#_(m/defmethod select/select* :before [:default :people/limit-2 :toucan2/honeysql]
  [_ _ query _]
  (assoc query :limit 2))

#_(deftest pre-select-test
  (testing "Should be able to do cool stuff in pre-select (select* :before)"
    (test/with-default-connection
      (is (= [(instance/instance :people/limit-2 {:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56Z")})
              (instance/instance :people/limit-2 {:id 2, :name "Sam", :created-at (t/offset-date-time "2019-01-11T23:56Z")})]
             (select/select :people/limit-2))))))

#_(m/defmethod select/select* :after [:default :people/no-timestamps :default]
  [connectable tableable reducible-query options]
  (testing "should not be an eduction yet -- if it is it means this method is getting called more than once"
    (is (not (instance? clojure.core.Eduction reducible-query))))
  (assert (not (instance? clojure.core.Eduction reducible-query)))
  (eduction
   (map (fn [person]
          (testing "select* :after should see Toucan 2 instances"
            (is (instance/toucan2-instance? person)))
          (testing "instance table should be a :people/no-timestamps"
            (is (isa? (instance/tableable person) :people/no-timestamps)))
          (dissoc person :timestamp)))
   reducible-query))

#_(deftest post-select-test
  (testing "Should be able to do cool stuff in (select* :after)"
    (test/with-default-connection
      (is (= [(instance/instance :people/no-timestamps {:id 1, :name "Cam"})
              (instance/instance :people/no-timestamps {:id 2, :name "Sam"})
              (instance/instance :people/no-timestamps {:id 3, :name "Pam"})
              (instance/instance :people/no-timestamps {:id 4, :name "Tam"})]
             (select/select :people/no-timestamps))))))

#_(derive :people/no-timestamps-limit-2 :people/no-timestamps)
#_(derive :people/no-timestamps-limit-2 :people/limit-2)

#_(deftest combine-aux-methods-test
  (is (= [(instance/instance :people/no-timestamps-limit-2 {:id 1, :name "Cam"})
          (instance/instance :people/no-timestamps-limit-2 {:id 2, :name "Sam"})]
         (select/select [:test/postgres :people/no-timestamps-limit-2]))))

(deftest select-one-test
  (is (= (instance/instance ::test/people {:id 1, :name "Cam", :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")})
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
        (is (= [(java.time.OffsetDateTime/parse "2020-04-21T23:56Z")
                (java.time.OffsetDateTime/parse "2019-01-11T23:56Z")
                (java.time.OffsetDateTime/parse "2020-01-01T21:56Z")
                (java.time.OffsetDateTime/parse "2020-05-25T19:56Z")]
               (select/select-fn-vec k ::test/people {:order-by [[:id :asc]]}))))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-fn-set :id ::test/people :id 100)))
    (is (nil? (select/select-fn-vec :id ::test/people :id 100)))))

#_(deftest select-one-fn-test
  (is (= 1
         (select/select-one-fn :id ::test/people :name "Cam"))))

#_(deftest select-pks-test
  (is (= #{1 2 3 4}
         (select/select-pks-set ::test/people)))
  (is (= [1 2 3 4]
         (select/select-pks-vec ::test/people)))
  (testing "non-integer PK"
    (is (= #{"Cam" "Sam" "Pam" "Tam"}
           (select/select-pks-set [:test/postgres :people/name-is-pk])))
    (is (= ["Cam" "Sam" "Pam" "Tam"]
           (select/select-pks-vec [:test/postgres :people/name-is-pk]))))
  (testing "Composite PK -- should return vectors"
    (is (= #{[1 "Cam"] [2 "Sam"] [3 "Pam"] [4 "Tam"]}
           (select/select-pks-set [:test/postgres :people/composite-pk])))
    (is (= [[1 "Cam"] [2 "Sam"] [3 "Pam"] [4 "Tam"]]
           (select/select-pks-vec [:test/postgres :people/composite-pk]))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-pks-set ::test/people :id 100)))
    (is (nil? (select/select-pks-vec ::test/people :id 100)))))

#_(deftest select-one-pk-test
  (is (= 1
         (select/select-one-pk ::test/people :name "Cam")))
  (testing "non-integer PK"
    (is (= "Cam"
           (select/select-one-pk [:test/postgres :people/name-is-pk] :id 1))))
  (testing "Composite PK -- should return vector"
    (is (= [1 "Cam"]
           (select/select-one-pk [:test/postgres :people/composite-pk] :id 1)))))

#_(deftest select-fn->fn-test
  (is (= {1 "Cam", 2 "Sam", 3 "Pam", 4 "Tam"}
         (select/select-fn->fn :id :name ::test/people)))
  (is (= {2 "cam", 3 "sam", 4 "pam", 5 "tam"}
         (select/select-fn->fn (comp inc :id) (comp str/lower-case :name) ::test/people)))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-fn->fn :id :name ::test/people :id 100)))))

#_(deftest select-pk->fn-test
  (is (= {1 "Cam", 2 "Sam", 3 "Pam", 4 "Tam"}
         (select/select-pk->fn :name ::test/people)))
  (is (= {1 "cam", 2 "sam", 3 "pam", 4 "tam"}
         (select/select-pk->fn (comp str/lower-case :name) ::test/people)))
  (testing "Composite PKs"
    (is (= {[1 "Cam"] "Cam", [2 "Sam"] "Sam", [3 "Pam"] "Pam", [4 "Tam"] "Tam"}
           (select/select-pk->fn :name [:test/postgres :people/composite-pk]))))
  (testing "Should return nil if the result is empty"
    (is (nil? (select/select-pk->fn :name ::test/people :id 100)))))

#_(deftest select-fn->pk-test
  (is (= {"Cam" 1, "Sam" 2, "Pam" 3, "Tam" 4}
         (select/select-fn->pk :name ::test/people)))
  (is (= {"cam" 1, "sam" 2, "pam" 3, "tam" 4}
         (select/select-fn->pk (comp str/lower-case :name) ::test/people)))
  (testing "Composite PKs"
    (is (= {"Cam" [1 "Cam"], "Sam" [2 "Sam"], "Pam" [3 "Pam"], "Tam" [4 "Tam"]}
  (testing "Should return nil if the result is empty"
           (select/select-fn->pk :name [:test/postgres :people/composite-pk]))))
    (is (nil? (select/select-fn->pk :name [:test/postgres :people/composite-pk] :id 100)))))

#_(deftest count-test
  (is (= 4
         (select/count ::test/people)))
  (is (= 1
         (select/count ::test/people 1)))
  (is (= 3
         (select/count [:test/postgres :venues])))
  (is (= 2
         (select/count [:test/postgres :venues] :category "bar"))))

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

#_(m/defmethod conn.current/default-connectable-for-tableable* :people/custom-instance-type
  [_ _]
  :test/postgres)

#_(deftest custom-instance-type-test
  (let [m (select/select-one :people/custom-instance-type 1)]
    (is (= {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
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
