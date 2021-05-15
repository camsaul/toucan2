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

(derive ::transformed-venues-id-is-string ::transformed-venues)

(m/defmethod transformed/transforms* [:default ::transformed-venues-id-is-string]
  [connectable tableable options]
  (merge
   {:id {:in  #(Integer/parseInt ^String %)
         :out str}}
   (next-method connectable tableable options)))

(deftest select-in-test
  (testing "select should transform values going in"
    (testing "value is a pk condition"
      (testing "k-v condition"
        (is (= [{:id 1, :name "Tempest", :category :bar}
                {:id 2, :name "Ho's Tavern", :category :bar}]
               (select/select [:test/postgres ::transformed-venues] :category :bar {:select [:id :name :category]}))))
      (testing "as the PK"
        (testing "(single value)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select [:test/postgres ::transformed-venues-id-is-string] "1" {:select [:id :name :category]}))))
        (testing "(vector of multiple values)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select [:test/postgres ::transformed-venues-id-is-string] ["1"] {:select [:id :name :category]}))))))))

(deftest select-out-test
  (testing "select should transform values coming out"
    (is (= [{:id 1, :name "Tempest", :category :bar}
            {:id 2, :name "Ho's Tavern", :category :bar}
            {:id 3, :name "BevMo", :category :store}]
           (select/select [:test/postgres ::transformed-venues] {:select [:id :name :category]})))
    (testing "should work if transformed key is not present in results"
      (is (= [{:id 1, :name "Tempest"}
              {:id 2, :name "Ho's Tavern"}
              {:id 3, :name "BevMo"}]
             (select/select [:test/postgres ::transformed-venues] {:select [:id :name]}))))
    (testing "should work with select-one and other special functions"
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
