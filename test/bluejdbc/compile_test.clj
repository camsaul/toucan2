(ns bluejdbc.compile-test
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.tableable :as tableable]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(deftest string-test
  (testing "Compiling a plain string should no-op"
    (is (= ["SELECT 1 AS one;"]
           (compile/compile :test/postgres "SELECT 1 AS one;")))
    (is (= ["SELECT ?, ?, ?;" 1 2 3]
           (compile/compile :test/postgres ["SELECT ?, ?, ?;" 1 2 3])))))

(deftest honeysql-test
  (is (= ["SELECT * FROM people WHERE id = ?" 1]
         (compile/compile :test/postgres {:select [:*], :from [:people], :where [:= :id 1]})))
  (testing "Should be able to pass a HoneySQL map as first arg in a query-params vector"
    (is (= ["SELECT * FROM people WHERE id = ?" 1 2]
           (compile/compile :test/postgres [{:select [:*], :from [:people], :where [:= :id 1]} 2])))))

(m/defmethod compile/compile* [:default ::people-count]
  [connectable _ options]
  (compile/compile connectable {:select [:%count.*], :from [:people]} options))

(deftest compile-named-query-test
  (is (= ["SELECT count(*) FROM people"]
         (compile/compile :test/postgres ::people-count))))

(deftest compile-honeysql-options-test
  (testing "Should be able to pass `:honeysql` options"
    (is (= ["SELECT * FROM \"people\" WHERE \"my-id\" = ?" 1]
           (compile/compile :test/postgres
                            {:select [:*]
                             :from   [:people]
                             :where  [:= :my-id 1]}
                            {:honeysql {:quoting             :ansi
                                        :allow-dashed-names? true}}))))
  (testing "Should pick up options from the connectable"
    (is (= ["SELECT * FROM \"people\""]
           (compile/compile :test/postgres-with-quoting {:select [:*] :from [:people]}))))
  (testing "Options passed directly to `compile` should override those from the connectable"
    (is (= ["SELECT * FROM `people`"]
           (compile/compile :test/postgres-with-quoting {:select [:*] :from [:people]} {:honeysql {:quoting :mysql}})))))

(m/defmethod tableable/table-name* [:default ::my-amazing-table]
  [_ _ _]
  "wow")

(deftest table-identifier-test
  (is (= (compile/table-identifier :people nil)
         (compile/table-identifier :people))))

(deftest from-test
  (is (= {:select [:*]
          :from   [(compile/table-identifier :people)]}
         (compile/from :people {:select [:*]})))
  (is (= ["SELECT * FROM people"]
         (compile/compile (compile/from :people {:select [:*]}))))

  (testing "Should use options passed directly to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile nil (compile/from :people {:select [:field]}) {:honeysql {:quoting :ansi}}))))

  (testing "Should use options from the `connnectable` passed to `compile`"
    (is (= ["SELECT \"field\" FROM \"people\""]
           (compile/compile :test/postgres-with-quoting (compile/from :people {:select [:field]})))))

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
           (compile/compile (compile/from ::my-amazing-table {:select [:*]}))))))
