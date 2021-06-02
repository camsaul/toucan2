(ns bluejdbc.honeysql-util-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.select :as select]
            [bluejdbc.select-test :as select-test]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]))

(comment select-test/keep-me)

(use-fixtures :once test/do-with-test-data)

(deftest custom-condition-handler-test
  (testing "single PKs"
    (testing "(unwrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :bluejdbc/with-pks [1 2] {:select [:id :name], :order-by [[:id :asc]]}))))
    (testing "(wrapped)"
      (is (= [(instance/instance :people {:id 1, :name "Cam"})
              (instance/instance :people {:id 2, :name "Sam"})]
             (select/select [:test/postgres :people] :bluejdbc/with-pks [[1] [2]] {:select [:id :name], :order-by [[:id :asc]]})))))
  (testing "composite PKs"
    (is (= [(instance/instance :people/composite-pk {:id 1, :name "Cam"})]
           (select/select [:test/postgres :people/composite-pk] :bluejdbc/with-pks [[1 "Cam"] [2 "Wham"]] {:select [:id :name], :order-by [[:id :asc]]})))))
