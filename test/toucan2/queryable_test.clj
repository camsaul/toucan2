(ns toucan2.queryable-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.queryable :as queryable]))

(m/defmethod queryable/queryable* [:default :default ::named-query]
  [_ _ _ _]
  {:select [:*], :from [:people]})

(deftest named-query-test
  (is (= {:select [:*], :from [:people]}
         (queryable/queryable ::named-query))))

(deftest queryable?-test
  (is (queryable/queryable? nil nil ::named-query))
  (is (queryable/queryable? nil nil {}))
  (is (queryable/queryable? nil nil "SELECT *"))
  (is (not (queryable/queryable? nil nil ::another-named-query))))
