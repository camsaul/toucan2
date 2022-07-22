(ns toucan2.realize-test
  (:require
   [clojure.test :refer :all]
   [toucan2.realize :as realize]))

(deftest realize-test
  (is (= [:a :b [:a :b :c]]
         (realize/realize (reify clojure.lang.IReduceInit
                            (reduce [_this rf init]
                              (reduce rf init [:a :b (reify clojure.lang.IReduceInit
                                                       (reduce [_this rf init]
                                                         (reduce rf init [:a :b :c])))])))))))
