(ns fourcan.db-test
  (:require [clojure.test :refer :all]
            [fourcan
             [db :as db]
             [test-util :as test.u]
             [types :as types]]))

(deftest select-test
  (is (= (types/query :bird {:}) (db/select :bird))))

(deftest select-test
  (test.u/with-test-data-source
    (test.u/create-birds-table!)
    (is (= [[1 "Toucan"] [2 "Pigeon"] [3 "Pelican"]]
           (seq (db/select :bird))))))
