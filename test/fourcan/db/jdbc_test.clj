(ns fourcan.db.jdbc-test
  (:require [clojure.test :refer :all]
            [fourcan.db.jdbc :as jdbc]
            [fourcan.test-util :as test.u]))

(deftest basic-test
  (test.u/with-test-data-source
    (test.u/create-birds-table!)
    (is (= [[1 "Toucan"] [2 "Pigeon"] [3 "Pelican"]]
           (jdbc/query nil "SELECT \"id\", \"species\" FROM \"bird\";" nil)
           (jdbc/query nil {:select [:id :species], :from [:bird]} nil)
           (jdbc/query :bird {:select [:id :species], :from [:bird]} nil)))
    (testing "Make sure we can set parameters correctly"
      (is (= [[2 "Pigeon"]]
             (jdbc/query :bird {:select [:id :species], :from [:bird], :where [:= :id 2]} nil))))))
