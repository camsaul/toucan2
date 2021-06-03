(ns bluejdbc.compile-test
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.tableable :as tableable]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(deftest compile-string-test
  (testing "Compiling a plain string should no-op"
    (is (= ["SELECT 1 AS one;"]
           (compile/compile :test/postgres nil "SELECT 1 AS one;")))
    (is (= ["SELECT ?, ?, ?;" 1 2 3]
           (compile/compile :test/postgres nil ["SELECT ?, ?, ?;" 1 2 3])))))

(deftest compile-honeysql-test
  (is (= ["SELECT * FROM people WHERE id = ?" 1]
         (compile/compile :test/postgres nil {:select [:*], :from [:people], :where [:= :id 1]})))
  (testing "Should be able to pass a HoneySQL map as first arg in a query-params vector"
    (is (= ["SELECT * FROM people WHERE id = ?" 1 2]
           (compile/compile :test/postgres nil [{:select [:*], :from [:people], :where [:= :id 1]} 2])))))

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  {:select [:%count.*], :from [:people]})

(deftest compile-named-query-test
  (is (= ["SELECT count(*) FROM people"]
         (compile/compile :test/postgres nil ::named-query))))

(deftest compile-honeysql-options-test
  (testing "Should be able to pass `:honeysql` options"
    (is (= ["SELECT * FROM \"people\" WHERE \"my-id\" = ?" 1]
           (compile/compile :test/postgres
                            nil
                            {:select [:*]
                             :from   [:people]
                             :where  [:= :my-id 1]}
                            {:honeysql {:quoting             :ansi
                                        :allow-dashed-names? true}}))))
  (testing "Should pick up options from the connectable"
    (is (= ["SELECT * FROM \"people\""]
           (compile/compile :test/postgres-with-quoting nil {:select [:*] :from [:people]}))))
  (testing "Options passed directly to `compile` should override those from the connectable"
    (is (= ["SELECT * FROM `people`"]
           (compile/compile :test/postgres-with-quoting nil {:select [:*] :from [:people]} {:honeysql {:quoting :mysql}})))))

(m/defmethod tableable/table-name* [:default ::my-amazing-table]
  [_ _ _]
  "wow")

(deftest table-identifier-test
  (is (= (compile/table-identifier :people nil)
         (compile/table-identifier :people))))

(m/defmethod queryable/queryable* [:default :default ::named-query-no-from]
  [_ _ _ _]
  {:select [:%count.*]})

(deftest from-test
  (is (= {:select [:*]
          :from   [(compile/table-identifier :people)]}
         (compile/from :people {:select [:*]})))
  (is (= ["SELECT * FROM people"]
         (compile/compile (compile/from :people {:select [:*]}))))

  (testing "Should use options passed directly to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile nil nil (compile/from :people {:select [:field]}) {:honeysql {:quoting :ansi}}))))

  (testing "Should use options from the `connnectable` passed to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile :test/postgres-with-quoting nil (compile/from :people {:select [:field]})))))

  (testing "Should be able to pass options to the TableIdentifier itself"
    (is (= {:select [:field]
            :from   [(compile/table-identifier :people {:honeysql {:quoting :ansi}})]}
           (compile/from nil :people {:select [:field]} {:honeysql {:quoting :ansi}})))
    (is (= ["SELECT field FROM \"people\""]
           (compile/compile (compile/from nil :people {:select [:field]} {:honeysql {:quoting :ansi}})))))

  (testing "Options passed directly to `from` should override `compile` options")

  (testing "Should use the namespace of a qualified keyword"
    (is (= ["SELECT * FROM people"]
           (compile/compile (compile/from :people/all-columns {:select [:*]})))))

  (testing "Should work with a tableable that implements table-name"
    (is (= {:select [:*]
            :from   [(compile/table-identifier ::my-amazing-table)]}
           (compile/from ::my-amazing-table {:select [:*]})))
    (is (= ["SELECT * FROM wow"]
           (compile/compile (compile/from ::my-amazing-table {:select [:*]})))))

  (testing "named query"
    (is (= "wow"
           (tableable/table-name ::my-amazing-table)))
    (is (= ["SELECT count(*) FROM wow"]
           (compile/compile (compile/from ::my-amazing-table ::named-query-no-from))))
    (testing "If named query already has :from, don't stomp on it"
      (is (= ["SELECT count(*) FROM people"]
             (compile/compile (compile/from ::my-amazing-table ::named-query)))))))
