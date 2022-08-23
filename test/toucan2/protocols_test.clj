(ns toucan2.protocols-test
  (:require [clojure.test :refer :all]
            [toucan2.protocols :as protocols]))

;;; there are more tests in [[toucan2.instance-test]]

(deftest original-test
  (is (= nil
         (protocols/original {})))
  (is (= nil
         (protocols/original nil))))
