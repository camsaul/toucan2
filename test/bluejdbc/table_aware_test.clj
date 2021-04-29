(ns bluejdbc.table-aware-test
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(defn- test-people-instances? [results]
  (testing "All results should be :people instances"
    (is (every? (partial = :people) (map instance/table results)))))

(deftest query-as-test
  (let [results (query/all
                 (table-aware/reducible-query-as :test/postgres :people {:select [:*], :from [:people]} nil))]
    (test-people-instances? results)
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           results))))

(m/defmethod compile/compile* [:default ::ids]
  [connectable _ options]
  (compile/compile* connectable {:select [:id]} options))

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  {:named-query? true})

(deftest parse-select-args-test
  (doseq [[query expected-query] {[]                    nil
                                  [{:query true}]       {:query true}
                                  ["query"]             "query"
                                  [[{:query true} 1 2]] [{:query true} 1 2]
                                  [["query" 1 2]]       ["query" 1 2]
                                  [:keyword-query]      :keyword-query
                                  [::named-query]       {:named-query? true}}
          ;; can only have id if query is a map
          [id expected-id]       (when (or (map? (first query))
                                           (= (first query) ::named-query))
                                   {[]                 nil
                                    [1]                1
                                    [[1 2 3]]          [1 2 3]
                                    ;; anything besides a keyword or map should be allowed as an id.
                                    ["id"]             "id"
                                    ['id]              'id
                                    ;; maps and keywords are allowed inside vectors but not directly
                                    [[{:map-id true}]] [{:map-id true}]
                                    [[:keyword-id]]    [:keyword-id]
                                    [[1 "id"]]         [1 "id"]})
          ;; can only have kvs if query is a map
          [kvs expected-kvs]     (when (map? (first query))
                                   {[]                      nil
                                    [:k 1]                  {:k 1}
                                    [:k1 1, :k2 2]          {:k1 1, :k2, 2}
                                    [:k "v"]                {:k "v"}
                                    [:k1 1, :k2 2, :k3 "v"] {:k1 1, :k2 2, :k3 "v"}})

          ;; can only have options if you have a query.
          [options expected-options] (when (seq query)
                                       {[]                nil
                                        [{}]              {}
                                        [{:options true}] {:options true}})
          :let                       [args (vec (concat id kvs query options))]]
    (testing (pr-str (list `parse-select-args args))
      (is (= {:id      expected-id
              :kvs     expected-kvs
              :query   expected-query
              :options expected-options}
             (table-aware/parse-select-args :connectable :tableable args))))))

(deftest select-test
  (testing "no args"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")}]
           (table-aware/select [:test/postgres :people])))
    (test-people-instances? (table-aware/select [:test/postgres :people])))

  (testing "using current connection (dynamic binding)"
    (binding [conn/*connectable* :test/postgres]
      (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}
              {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")}
              {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")}
              {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")}]
             (table-aware/select :people)))))

  (testing "using default connection"
    (test/with-default-connection
      (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}
              {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")}
              {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")}
              {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")}]
             (table-aware/select :people)))))

  (testing "one arg (id)"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (table-aware/select [:test/postgres :people] 1))))

  (testing "one arg (query)"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (table-aware/select [:test/postgres :people] {:where [:= :id 1]}))))

  (testing "two args (k v)"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (table-aware/select [:test/postgres :people] :id 1)))
    (testing "sequential v"
      (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
             (table-aware/select [:test/postgres :people] :id [:= 1]))))))

(m/defmethod tableable/primary-key* [:default :people/name-is-pk]
  [_ _]
  :name)

(m/defmethod tableable/primary-key* [:default :people/composite-pk]
  [_ _]
  [:id :name])

(deftest select-non-integer-pks-test
  (testing "non-integer PK"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (table-aware/select [:test/postgres :people/name-is-pk] "Cam"))))

  (testing "composite PK"
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")}]
           (table-aware/select [:test/postgres :people/composite-pk] [1 "Cam"])))
    (is (= []
           (table-aware/select [:test/postgres :people/composite-pk] [2 "Cam"])
           (table-aware/select [:test/postgres :people/composite-pk] [1 "Sam"])))))

;; this could also be done as part a `:before` method.
(m/defmethod table-aware/select* [:default :people/no-timestamps clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (let [query (merge {:select [:id :name]}
                     query)]
    (next-method connectable tableable query options)))

(deftest default-query-test
  (testing "Should be able to set some defaults by implementing `select*`"
    (test/with-default-connection
      (is (= [(instance/instance :people/no-timestamps {:id 1, :name "Cam"})]
             (table-aware/select :people/no-timestamps 1))))))

(m/defmethod table-aware/select* :before [:default :people/limit-2 clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (assoc query :limit 2))

(deftest pre-select-test
  (testing "Should be able to do cool stuff in pre-select (select* :before)"
    (test/with-default-connection
      (is (= [(instance/instance :people/limit-2 {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")})
              (instance/instance :people/limit-2 {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")})]
             (table-aware/select :people/limit-2))))))

(m/defmethod table-aware/select* :after [:default :people/no-timestamps :default]
  [connectable tableable reducible-query options]
  (testing "should not be an eduction yet -- if it is it means this method is getting called more than once"
    (is (not (instance? clojure.core.Eduction reducible-query))))
  (assert (not (instance? clojure.core.Eduction reducible-query)))
  (eduction
   (map #(dissoc % :timestamp))
   reducible-query))

(deftest post-select-test
  (testing "Should be able to do cool stuff in (select* :after)"
    (test/with-default-connection
      (is (= [(instance/instance :people/no-timestamps {:id 1, :name "Cam"})
              (instance/instance :people/no-timestamps {:id 2, :name "Sam"})
              (instance/instance :people/no-timestamps {:id 3, :name "Pam"})
              (instance/instance :people/no-timestamps {:id 4, :name "Tam"})]
             (table-aware/select :people/no-timestamps))))))

(derive :people/no-timestamps-limit-2 :people/no-timestamps)
(derive :people/no-timestamps-limit-2 :people/limit-2)

(deftest combine-aux-methods-test
  (is (= [(instance/instance :people/no-timestamps-limit-2 {:id 1, :name "Cam"})
          (instance/instance :people/no-timestamps-limit-2 {:id 2, :name "Sam"})]
         (table-aware/select [:test/postgres :people/no-timestamps-limit-2]))))

(deftest select-one-test
  (is (= {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
         (table-aware/select-one [:test/postgres :people]))))
