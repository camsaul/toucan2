(ns bluejdbc.transformed-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [bluejdbc.transformed :as transformed]
            [clojure.test :refer :all]
            [java-time :as t]
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

(defn- parse-int [^String s]
  (Integer/parseInt ^String s))

(m/defmethod transformed/transforms* [:default ::transformed-venues-id-is-string]
  [connectable tableable options]
  (merge
   {:id {:in  parse-int
         :out str}}
   (next-method connectable tableable options)))

(deftest select-in-test
  (testing "select should transform values going in"
    (testing "key-value condition"
      (is (= [{:id 1, :name "Tempest", :category :bar}
              {:id 2, :name "Ho's Tavern", :category :bar}]
             (select/select [:test/postgres ::transformed-venues] :category :bar {:select   [:id :name :category]
                                                                                  :order-by [[:id :asc]]})))
      (testing "Toucan-style [f & args] condition"
        (is (= [{:id 1, :name "Tempest", :category :bar}
                {:id 2, :name "Ho's Tavern", :category :bar}]
               (select/select [:test/postgres ::transformed-venues] :category [:in [:bar]] {:select   [:id :name :category]
                                                                                            :order-by [[:id :asc]]})))))
    (testing "as the PK"
      (testing "(single value)"
        (is (= [{:id "1", :name "Tempest", :category :bar}]
               (select/select [:test/postgres ::transformed-venues-id-is-string] "1" {:select [:id :name :category]}))))
      (testing "(vector of multiple values)"
        (is (= [{:id "1", :name "Tempest", :category :bar}]
               (select/select [:test/postgres ::transformed-venues-id-is-string] ["1"] {:select [:id :name :category]})))))))

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
             (select/select-fn-set :category [:test/postgres ::transformed-venues]))))
    (testing "Transformed version of the map should be considered the instance 'original'"
      (let [instance (select/select-one [:test/postgres ::transformed-venues] 1 {:select [:id :name :category]})]
        (is (= {:id 1, :name "Tempest", :category :bar}
               (instance/original instance)))
        (is (= nil
               (instance/changes instance)))))))

(deftest update!-test
  (test/with-default-connection
    (testing "key-value conditions"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 2}
               (mutative/update! ::transformed-venues :category :bar {:category :BAR})))
        (is (= #{["Ho's Tavern" :BAR] ["Tempest" :BAR]}
               (select/select-fn-set (juxt :name :category) ::transformed-venues :category :BAR))))
      (testing "Toucan-style [f & args] condition"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 2}
                 (mutative/update! ::transformed-venues :category [:in [:bar]] {:category :BAR})))
          (is (= #{:store :BAR}
                 (select/select-fn-set :category ::transformed-venues))))))
    (testing "conditions map"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 2}
               (mutative/update! ::transformed-venues {:category :bar} {:category :BAR})))))
    (testing "PK"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/update! ::transformed-venues-id-is-string "1" {:name "Wow"})))
        (is (= "Wow"
               (select/select-one-fn :name ::transformed-venues 1)))))))

(deftest insert!-test
  (test/with-default-connection
    (testing "single map row"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! ::transformed-venues {:name "Hi-Dive", :category :bar})))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues :category :bar)))))
    (testing "multiple map rows"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! ::transformed-venues [{:name "Hi-Dive", :category :bar}])))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues :category :bar)))))
    (testing "kv args"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! ::transformed-venues :name "Hi-Dive", :category :bar)))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues :category :bar)))))
    (testing "columns + vector rows"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! ::transformed-venues [:name :category] [["Hi-Dive" :bar]])))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues :category :bar)))))
    (testing "returning-keys"
      (test/with-venues-reset
        (is (= ["4"]
               (mutative/insert-returning-keys! ::transformed-venues-id-is-string [{:name "Hi-Dive", :category "bar"}])))))))

(deftest save!-test
  (test/with-venues-reset
    (test/with-default-connection
      (let [venue (select/select-one ::transformed-venues 1)]
        (is (= {:next.jdbc/update-count 1}
               (mutative/save! (assoc venue :category :dive-bar))))
        (is (= {:id         1
                :name       "Tempest"
                :category   :dive-bar
                :created-at (t/local-date-time "2017-01-01T00:00")
                :updated-at (t/local-date-time "2017-01-01T00:00")}
               (select/select-one ::transformed-venues 1)))))))

(deftest delete!-test
  (test/with-default-connection
    (testing "Delete row by PK"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/delete! ::transformed-venues-id-is-string "1")))
        (is (= []
               (select/select :venues 1)))
        (is (= #{2}
               (select/select-fn-set :id :venues 2)))))
    (testing "Delete row by key-value conditions"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 2}
               (mutative/delete! ::transformed-venues :category :bar)))
        (is (= []
               (select/select ::transformed-venues :category :bar))))
      (testing "Toucan-style fn-args vector"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 2}
                 (mutative/delete! ::transformed-venues :category [:in [:bar]])))
          (is (= []
                 (select/select ::transformed-venues :category :bar))))))))
