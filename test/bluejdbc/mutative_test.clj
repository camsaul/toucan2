(ns bluejdbc.mutative-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(use-fixtures :once test/do-with-test-data)

(deftest parse-update-args-test
  (is (= {:id 1, :changes {:a 1}, :conditions nil}
         (mutative/parse-update!-args* nil nil [1 {:a 1}] nil)))
  (is (= {:conditions {:id 1}, :changes {:a 1}}
         (mutative/parse-update!-args* nil nil [:id 1 {:a 1}] nil)))
  (is (= {:id 1, :conditions {:name "Cam"}, :changes {:a 1}}
         (mutative/parse-update!-args* nil nil [1 :name "Cam" {:a 1}] nil)))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}}
         (mutative/parse-update!-args* nil nil [{:id 1} {:name "Hi-Dive"}] nil)))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}, :options {:options? true}}
         (mutative/parse-update!-args* nil nil [{:id 1} {:name "Hi-Dive"} {:options? true}] nil))))

(deftest update!-test
  (test/with-venues-reset
    (is (= {:next.jdbc/update-count 1}
           (mutative/update! [:test/postgres :venues] 1 {:name "Hi-Dive"})))
    (is (= (instance/instance :venues {:id         1
                                       :name       "Hi-Dive"
                                       :category   "bar"
                                       :created-at (t/local-date-time "2017-01-01T00:00")
                                       :updated-at (t/local-date-time "2017-01-01T00:00")})
           (select/select-one [:test/postgres :venues] 1)))))

(deftest save!-test
  (test/with-venues-reset
    (test/with-default-connection
      (let [venue (select/select-one :venues 1)]
        (is (= {:next.jdbc/update-count 1}
               (mutative/save! (assoc venue :name "Hi-Dive"))))
        (is (= (instance/instance :venues {:id         1
                                           :name       "Hi-Dive"
                                           :category   "bar"
                                           :created-at (t/local-date-time "2017-01-01T00:00")
                                           :updated-at (t/local-date-time "2017-01-01T00:00")})
               (select/select-one :venues 1)))))))

(deftest parse-insert!-args-test
  (testing "single map row"
    (is (= {:rows [{:row 1}]}
           (mutative/parse-insert!-args* nil nil [{:row 1}] nil)))
    (is (= {:rows [{:row 1}], :options {:options? true}}
           (mutative/parse-insert!-args* nil nil [{:row 1} {:options? true}] nil))))
  (testing "multiple map rows"
    (is (= {:rows [{:row 1} {:row 2}]}
           (mutative/parse-insert!-args* nil nil [[{:row 1} {:row 2}]] nil)))
    (is (= {:rows [{:row 1} {:row 2}], :options {:options? true}}
           (mutative/parse-insert!-args* nil nil [[{:row 1} {:row 2}] {:options? true}] nil))))
  (testing "kv args"
    (is (= {:rows [{:a 1, :b 2, :c 3}]}
           (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c 3] nil)))
    (is (= {:rows [{:a 1, :b 2, :c 3}], :options {:options? true}}
           (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c 3 {:options? true}] nil)))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Don't know how to interpret insert\! args: \(\) - failed: Insufficient input"
         (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c] nil))))
  (testing "columns + vector rows"
    (is (= {:rows [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}]}
           (mutative/parse-insert!-args* nil nil [[:a :b :c] [[1 2 3] [4 5 6]]] nil)))
    (is (= {:rows [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}], :options {:options? true}}
           (mutative/parse-insert!-args* nil nil [[:a :b :c] [[1 2 3] [4 5 6]] {:options? true}] nil)))))

(deftest insert!-test
  (test/with-venues-reset
    (test/with-default-connection
      (is (= nil
             (select/select-one :venues 4)))
      (testing "Insert a single row"
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! :venues {:name "Grant & Green", :category "bar"})))
        (is (= (instance/instance :venues {:id         4
                                           :name       "Grant & Green"
                                           :category   "bar"
                                           :created-at (t/local-date-time "2017-01-01T00:00")
                                           :updated-at (t/local-date-time "2017-01-01T00:00")})
               (select/select-one :venues 4))))

      (testing "Insert multiple rows"
        (is (= {:next.jdbc/update-count 2}
               (mutative/insert! :venues [{:name "Black Horse London Pub", :category "bar"}
                                          {:name "Nick's Crispy Tacos", :category "bar"}])))
        (is (= [(instance/instance :venues {:id         5
                                            :name       "Black Horse London Pub"
                                            :category   "bar"
                                            :created-at (t/local-date-time "2017-01-01T00:00")
                                            :updated-at (t/local-date-time "2017-01-01T00:00")})
                (instance/instance :venues {:id         6
                                            :name       "Nick's Crispy Tacos"
                                            :category   "bar"
                                            :created-at (t/local-date-time "2017-01-01T00:00")
                                            :updated-at (t/local-date-time "2017-01-01T00:00")})]
               (select/select :venues :id [:> 4] {:order-by [[:id :asc]]}))))

      (testing "Insert with key/values"
        (is (= {:next.jdbc/update-count 1}
               (mutative/insert! :venues :name "HiDive SF", :category "bar")))
        (is (= (instance/instance :venues {:id         7
                                           :name       "HiDive SF"
                                           :category   "bar"
                                           :created-at (t/local-date-time "2017-01-01T00:00")
                                           :updated-at (t/local-date-time "2017-01-01T00:00")})
               (select/select-one :venues :id 7))))

      (testing "Insert with column names"
        (is (= {:next.jdbc/update-count 2}
               (mutative/insert! "venues" [:name :category] [["The Ramp" "bar"]
                                                             ["Louie's" "bar"]])))
        (is (= [(instance/instance "venues" {:id 8, :name "The Ramp"})
                (instance/instance "venues" {:id 9, :name "Louie's"})]
               (select/select "venues" :id [:> 7] {:select [:id :name], :order-by [[:id :asc]]})))))))
