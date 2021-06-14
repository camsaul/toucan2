(ns toucan2.queryable-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.queryable :as queryable]
            [toucan2.util :as u]))

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  {:select [:*], :from [:people]})

(deftest named-query-test
  (is (= {:select [:*], :from [:people]}
         (queryable/queryable ::named-query))))

(deftest queryable?-test
  (is (queryable/queryable? nil nil ::named-query))
  (testing "maps"
    (testing "should not be queryable out of the box"
      (is (not (queryable/queryable? nil nil {}))))
    (testing "SHOULD be queryable if"
      (testing "connectable derives from a query compilation backend"
        (is (queryable/queryable? :test/postgres nil "SELECT *")))
      (testing "tableable derives from a query compilation backend"
        (is (queryable/queryable? nil (u/dispatch-on nil :toucan2/honeysql) "SELECT *")))))
  (is (not (queryable/queryable? nil nil ::another-named-query))))
