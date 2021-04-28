(ns bluejdbc.table-aware-test
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.instance :as instance]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(defn- test-people-instances? [results]
  (testing "All results should be :people instances"
    (is (every? (partial = :people) (map instance/table results)))))

(deftest query-as-test
  (let [results (table-aware/query-as :test/postgres :people {:select [:*], :from [:people]} nil)]
    (test-people-instances? results)
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           results))))

(m/defmethod compile/compile* [:default ::ids]
  [connectable _ options]
  (compile/compile* connectable {:select [:id]} options))

(deftest select*-test
  (doseq [[message thunk] {"should be able to do a HoneySQL query"
                           #(table-aware/select* :test/postgres :people {:select [:id]} nil)

                           "should be able to do a plain SQL query"
                           #(table-aware/select* :test/postgres :people "SELECT id FROM people" nil)}]
    (testing message
      (let [results (thunk)]
        (test-people-instances? results)
        (is (= [{:id 1}
                {:id 2}
                {:id 3}
                {:id 4}]
               results))))))

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
