(ns bluejdbc.transformed-test
  (:require [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [bluejdbc.transformed :as transformed]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(m/defmethod tableable/table-name* [:default ::transformed-venues]
  [_ _ _]
  "venues")

(derive ::transformed-venues :bluejdbc/transformed)

(m/defmethod transformed/transforms* [:default ::transformed-venues]
  [_ _ _]
  {:category {:in  name
              :out keyword}})

(deftest select-in-test
  (testing "in (select)"
    (is (= [{:id 1, :name "Tempest", :category :bar}
            {:id 2, :name "Ho's Tavern", :category :bar}]
           (select/select [:test/postgres ::transformed-venues] :category :bar {:select [:id :name :category]})))
    (testing "if condition is not present"
      (is (= [{:id 1, :name "Tempest", :category :bar}
              {:id 2, :name "Ho's Tavern", :category :bar}
              {:id 3, :name "BevMo", :category :store}]
             (select/select [:test/postgres ::transformed-venues] {:select [:id :name :category]}))))))

(deftest select-out-test
  (testing "out (select)"
    (is (= [{:id 1, :name "Tempest", :category :bar}
            {:id 2, :name "Ho's Tavern", :category :bar}
            {:id 3, :name "BevMo", :category :store}]
           (select/select [:test/postgres ::transformed-venues] {:select [:id :name :category]})))
    (testing "if value is not present"
      (is (= [{:id 1, :name "Tempest"}
              {:id 2, :name "Ho's Tavern"}
              {:id 3, :name "BevMo"}]
             (select/select [:test/postgres ::transformed-venues] {:select [:id :name]}))))
    (testing "select-one-fn and other special methods"
      (is (= :bar
             (select/select-one-fn :category [:test/postgres ::transformed-venues] :id 1)))
      (is (= #{:store :bar}
             (select/select-fn-set :category [:test/postgres ::transformed-venues]))))))

(deftest update-test
  (testing "in (update!)"))

(deftest insert-test
  (testing "in (insert!)"))

(deftest save!-test
  (testing "in (save!)"))
