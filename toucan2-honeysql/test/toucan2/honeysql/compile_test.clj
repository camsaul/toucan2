(ns toucan2.honeysql.compile-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.honeysql.compile :as honeysql.compile]))

(deftest table-identifier-test
  (is (= (honeysql.compile/table-identifier :people nil)
         (honeysql.compile/table-identifier :people))))

(m/defmethod honeysql.compile/to-sql* [:default ::my-table :k2 :default]
  [_ _ _ v _]
  v)

(deftest maybe-wrap-value-test
  (testing "value should only wrap keys in `Value`s if an impl for `to-sql*` exists."
    (is (= 100
           (honeysql.compile/maybe-wrap-value :test/postgres ::my-table :k1 100)))
    (is (= (honeysql.compile/value :test/postgres ::my-table :k2 100)
           (honeysql.compile/maybe-wrap-value :test/postgres ::my-table :k2 100)))))
