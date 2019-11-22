(ns fourcan.db-test
  (:require [clojure.test :refer :all]
            [fourcan
             [db :as db]
             [test-util :as test.u]
             [types :as types]]))

#_[model-or-object pk-value-or-honeysql-form? & options]

(deftest select-test
  (testing "one arg"
    (testing "(select model)"
      (is (= (types/query :bird {:select [:*], :from [:bird]})
             (db/select :bird))))
    (testing "(select instance)"
      (is (= (types/query :bird {:select [:*], :from [:bird], :where [:= :id 20]})
             (db/select (types/instance :bird {:id 20})))))
    ;; TODO
    (testing "(select query)"))
  ;; TODO
  (testing "two args"
    (testing "(select model pk-value)")
    (testing "(select model hsql-form)")
    (testing "(select instance pk-value)")
    (testing "(select instance hsql-form)"))
  ;; TODO
  (testing "three+ args"
    (is (= (types/query :bird {:where [:= :id 20]})
           (db/select :bird :id 20)))))

(deftest e2e-select-test
  (test.u/with-test-data-source
    (test.u/create-birds-table!)
    (is (= [[1 "Toucan"] [2 "Pigeon"] [3 "Pelican"]]
           (seq (db/select :bird))))))

(deftest select-one-test
  (test.u/with-test-data-source
    (test.u/create-birds-table!)
    (is (= [1 "Toucan"]
           (db/select-one :bird)))))
