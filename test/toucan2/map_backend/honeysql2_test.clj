(ns toucan2.map-backend.honeysql2-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(deftest ^:parallel condition->honeysql-where-clause-test
  (are [k v expected] (= expected
                         (map.honeysql/condition->honeysql-where-clause k v))
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

(deftest table-and-alias-test
  (are [model expected] (= expected
                           (map.honeysql/table-and-alias model))
    ::test/venues       [:venues]
    ::venues.namespaced [:venues :venue]
    "venues"            [:venues]))

;; (deftest identitfier-test
;;   (testing "Custom Honey SQL identifier clause"
;;     (are [identifier quoted? expected] (= expected
;;                                           (hsql/format {:select [:*], :from [[identifier]]}
;;                                                        {:quoted quoted?}))
;;       [::query/identifier :wow]          false ["SELECT * FROM wow"]
;;       [::query/identifier :wow]          true  ["SELECT * FROM \"wow\""]
;;       [::query/identifier :table :field] false ["SELECT * FROM table.field"]
;;       [::query/identifier :table :field] true  ["SELECT * FROM \"table\".\"field\""])))
