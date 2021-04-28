(ns bluejdbc.compile-test
  (:require [bluejdbc.compile :as compile]
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

(deftest named-query-test
  (is (= ["SELECT count(*) FROM people"]
         (compile/compile :test/postgres ::people-count))))

(deftest honeysql-options-test
  (testing "Should be able to pass `:honeysql` options"
    (is (= ["SELECT * FROM \"people\" WHERE \"my-id\" = ?" 1]
           (compile/compile :test/postgres
                            {:select [:*]
                             :from   [:people]
                             :where  [:= :my-id 1]}
                            {:honeysql {:quoting             :ansi
                                        :allow-dashed-names? true}})))))
