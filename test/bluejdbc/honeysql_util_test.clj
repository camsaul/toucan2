(ns bluejdbc.honeysql-util-test
  (:require [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.instance :as instance]
            [bluejdbc.select :as select]
            [bluejdbc.select-test :as select-test]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]))

(comment select-test/keep-me)

(use-fixtures :once test/do-with-test-data)

(deftest with-pks-test
  (testing "single PKs"
    (testing "(unwrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :bluejdbc/with-pks [1 2] {:select [:id :name], :order-by [[:id :asc]]})))
      (is (= {:query {:where [:in :id [1 2]]}, :options nil}
             (select/parse-select-args :test/postgres :people [:bluejdbc/with-pks [1 2]] nil))))
    (testing "(wrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :bluejdbc/with-pks [[1] [2]] {:select [:id :name], :order-by [[:id :asc]]})))))
  (testing "composite PKs"
    (is (= [(instance/instance :people/composite-pk {:id 1, :name "Cam"})]
           (select/select [:test/postgres :people/composite-pk] :bluejdbc/with-pks [[1 "Cam"] [2 "Wham"]] {:select [:id :name], :order-by [[:id :asc]]}))))
  (testing "nil or empty arg -- should-no-op"
    (is (= {:query {}, :options nil}
           (select/parse-select-args :test/postgres :people [:bluejdbc/with-pks nil] nil)
           (select/parse-select-args :test/postgres :people [:bluejdbc/with-pks []] nil)))))

(deftest merge-conditions-test
  (is (= {:select [:*], :where [:and [:= :id 1] [:in :id 2 3]]}
         (honeysql-util/merge-conditions {:select [:*], :where [:= :id 1]} nil nil {:id [:in 2 3]} nil)))
  (testing "empty conditions -- no-op"
    (is (= {:where [:= :id 1]}
           (honeysql-util/merge-conditions {:where [:= :id 1]} nil nil nil nil))))
  (testing "a custom condition"
    (is (= {:where [:and [:= :id 1] [:in :id [2 3]]]}
           (honeysql-util/merge-conditions {:where [:= :id 1]} nil nil {:bluejdbc/with-pks [[2] [3]]} nil)))))
