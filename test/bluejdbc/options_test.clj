(ns bluejdbc.options-test
  (:require [bluejdbc.options :as options]
            [clojure.test :refer :all]))

(deftest map->Properties-test
  (testing "converting a Clojure map to Properties"
    (testing "keys and values should be converted to strings"
      ;; Properties are equality-comparable to maps
      (is (= {"A" "true", "B" "false", "C" "100"}
             (options/->Properties {:A "true", "B" false, "C" 100}))))))
