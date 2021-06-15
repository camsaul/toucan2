(ns toucan2.compile-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.compile :as compile]
            [toucan2.queryable :as queryable]
            [toucan2.test :as test]))

(use-fixtures :once test/do-with-test-data)

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
  [connectable tableable _ options]
  (queryable/queryable* connectable tableable {:select [:%count.*], :from [:people]} options))

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
