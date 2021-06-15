(ns toucan2.honeysql.build-query-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.build-query :as build-query]
            [toucan2.compile :as compile]
            [toucan2.honeysql.compile :as honeysql.compile]
            [toucan2.instance :as instance]
            [toucan2.queryable :as queryable]
            [toucan2.select :as select]
            [toucan2.select-test :as select-test]
            [toucan2.tableable :as tableable]
            [toucan2.test :as test]))

(comment select-test/keep-me)

(use-fixtures :once test/do-with-test-data)

(deftest with-pks-test
  (testing "single PKs"
    (testing "(unwrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :toucan2/with-pks [1 2] {:select [:id :name], :order-by [[:id :asc]]})))
      (is (= {:query   {:select [:*]
                        :from   [(honeysql.compile/table-identifier :people)]
                        :where  [:in :id [1 2]]}
              :options nil}
             (select/parse-select-args :test/postgres :people [:toucan2/with-pks [1 2]] nil))))
    (testing "(wrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :toucan2/with-pks [[1] [2]] {:select [:id :name], :order-by [[:id :asc]]})))))
  (testing "composite PKs"
    (is (= [(instance/instance :people/composite-pk {:id 1, :name "Cam"})]
           (select/select [:test/postgres :people/composite-pk] :toucan2/with-pks [[1 "Cam"] [2 "Wham"]] {:select [:id :name], :order-by [[:id :asc]]}))))
  (testing "nil or empty arg -- should-no-op"
    (is (= {:query   {:select [:*]
                      :from   [(honeysql.compile/table-identifier :people)]}
            :options nil}
           (select/parse-select-args :test/postgres :people [:toucan2/with-pks nil] nil)
           (select/parse-select-args :test/postgres :people [:toucan2/with-pks []] nil)))))

(deftest merge-conditions-test
  (is (= {:select [:*], :where [:and [:= :id 1] [:in :id 2 3]]}
         (build-query/merge-kv-conditions*
          (build-query/maybe-buildable-query :test/postgres nil {:select [:*], :where [:= :id 1]} :select nil)
          {:id [:in 2 3]}
          nil)))
  (testing "empty conditions -- no-op"
    (is (= {:where [:= :id 1]}
           (build-query/merge-kv-conditions*
            (build-query/maybe-buildable-query :test/postgres nil {:where [:= :id 1]} :select nil)
            nil
            nil))))
  (testing "a custom condition"
    (is (= {:where [:and [:= :id 1] [:in :id [2 3]]]}
           (build-query/merge-kv-conditions*
            (build-query/maybe-buildable-query :test/postgres nil {:where [:= :id 1]} :select nil)
            {:toucan2/with-pks [[2] [3]]}
            nil)))))

(m/defmethod tableable/table-name* [:default ::my-amazing-table]
  [_ _ _]
  "wow")

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  "SELECT count(*) FROM people;")

(m/defmethod queryable/queryable* [:default :default ::named-query-no-from]
  [_ _ _ _]
  {:select [:%count.*]})

(defn with-table
  ([tableable queryable]
   (with-table nil tableable queryable nil))
  ([connectable tableable queryable]
   (with-table connectable tableable queryable nil))
  ([connectable tableable queryable options]
   (let [connectable (or connectable :test/postgres)
         query       (build-query/maybe-buildable-query connectable tableable queryable :select options)]
     (build-query/with-table* query tableable options))))

(deftest with-table-test
  (is (= {:select [:*]
          :from   [(honeysql.compile/table-identifier :people)]}
         (with-table :people {:select [:*]})))
  (is (= ["SELECT * FROM people"]
         (compile/compile (with-table :people {:select [:*]}))))

  (testing "Should use options passed directly to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile nil nil (with-table :people {:select [:field]}) {:honeysql {:quoting :ansi}}))))

  (testing "Should use options from the `connnectable` passed to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile :test/postgres-with-quoting nil (with-table :people {:select [:field]})))))

  (testing "Should be able to pass options to the TableIdentifier itself"
    (is (= {:select [:field]
            :from   [(honeysql.compile/table-identifier :people {:honeysql {:quoting :ansi}})]}
           (with-table nil :people {:select [:field]} {:honeysql {:quoting :ansi}})))
    (is (= ["SELECT field FROM \"people\""]
           (compile/compile (with-table nil :people {:select [:field]} {:honeysql {:quoting :ansi}})))))

  (testing "Options passed directly to `from` should override `compile` options")

  (testing "Should use the namespace of a qualified keyword"
    (is (= ["SELECT * FROM people"]
           (compile/compile (with-table :people/all-columns {:select [:*]})))))

  (testing "Should work with a tableable that implements table-name"
    (is (= {:select [:*]
            :from   [(honeysql.compile/table-identifier ::my-amazing-table)]}
           (with-table ::my-amazing-table {:select [:*]})))
    (is (= ["SELECT * FROM wow"]
           (compile/compile (with-table ::my-amazing-table {:select [:*]})))))

  (testing "named query"
    (is (= "wow"
           (tableable/table-name ::my-amazing-table)))
    (is (= ["SELECT count(*) FROM wow"]
           (compile/compile (with-table ::my-amazing-table ::named-query-no-from))))
    (testing "If named query already has :from, don't stomp on it"
      (is (= ["SELECT count(*) FROM people;"]
             (compile/compile (with-table ::my-amazing-table ::named-query)))))))
