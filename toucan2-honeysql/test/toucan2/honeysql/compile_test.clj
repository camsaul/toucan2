(ns toucan2.honeysql.compile-test
  (:require [clojure.test :refer :all]
            [toucan2.honeysql.compile :as honeysql.compile]))

(deftest table-identifier-test
  (is (= (honeysql.compile/table-identifier :people nil)
         (honeysql.compile/table-identifier :people))))
