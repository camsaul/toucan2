(ns toucan2.honeysql2-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.honeysql2 :as t2.honeysql]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(deftest ^:parallel condition->honeysql-where-clause-test
  (are [k v expected] (= expected
                         (t2.honeysql/condition->honeysql-where-clause k v))
    :id :id            [:= :id :id]
    1   :id            [:= 1 :id]
    :id 1              [:= :id 1]
    :a  1              [:= :a 1]
    :id [:> 1]         [:> :id 1]
    :a  [:between 1 2] [:between :a 1 2]
    :id [:in [1 2]]    [:in :id [1 2]]
    :id nil            [:= :id nil]))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::test/venues :venue})

(deftest ^:parallel table-and-alias-test
  (are [model expected] (= expected
                           (t2.honeysql/table-and-alias model))
    ::test/venues       [:venues]
    ::venues.namespaced [:venues :venue]
    "venues"            [:venues]))

(deftest ^:parallel maybe-qualify-columns-test
  (are [columns table-alias expected] (= expected
                                         (#'t2.honeysql/maybe-qualify-columns columns table-alias))
    [:a.b :a.c :a.d]  [:x]    [:a.b :a.c :a.d]    ; everything already qualified
    [:a/b :a/c :a/d]  [:x]    [:a/b :a/c :a/d]    ; everything already qualified (namespaced)
    [:b :a.c :d]      [:x]    [:x/b :a.c :x/d]    ; some things unqualified
    [:b :a.c :d]      [:x :y] [:y/b :a.c :y/d]    ; [table alias] instead of [table] -- qualify with alias
    [:b :a.c [:d :e]] [:x :y] [:y/b :a.c [:d :e]] ; ignore non-keywords
    [:b :a.c nil]     [:x :y] [:y/b :a.c nil]     ; ignore non-keywords
    [:a/b :a.c :d]    [:x]    [:a/b :a.c :x/d]))  ; ignore namespaced keywords
