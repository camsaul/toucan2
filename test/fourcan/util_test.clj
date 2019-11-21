(ns fourcan.util-test
  (:require [clojure.test :refer :all]
            [fourcan.util :as u]))

(deftest mapply-test
  (is (= []
         (u/mapply vector))
      "(mapply f) should be the same as (f)")
  (is (= [:a 1 :b 2]
         (u/mapply vector {:a 1, :b 2}))
      "the last arg should be applied as keyword args to f")
  (testing "only the last arg should be applied as keyword args"
    (is (= [:x :a 1 :b 2]
           (u/mapply vector :x {:a 1, :b 2})))
    (is (= [:x :y :a 1 :b 2]
           (u/mapply vector :x :y {:a 1, :b 2})))))
