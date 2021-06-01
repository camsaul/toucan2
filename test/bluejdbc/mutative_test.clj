(ns bluejdbc.mutative-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(m/defmethod tableable/primary-key* [:default :venues/composite-pk]
  [_ _]
  [:id :name])

(deftest parse-update-args-test
  (is (= {:pk 1, :changes {:a 1}, :conditions nil}
         (mutative/parse-update!-args* nil nil [1 {:a 1}] nil)))
  (is (= {:conditions {:id 1}, :changes {:a 1}}
         (mutative/parse-update!-args* nil nil [:id 1 {:a 1}] nil)))
  (testing "composite PK"
    (is (= {:pk [1 2], :changes {:a 1}, :conditions nil}
           (mutative/parse-update!-args* nil nil [[1 2] {:a 1}] nil))))
  (testing "key-value conditions"
    (is (= {:pk 1, :conditions {:name "Cam"}, :changes {:a 1}}
           (mutative/parse-update!-args* nil nil [1 :name "Cam" {:a 1}] nil))))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}}
         (mutative/parse-update!-args* nil nil [{:id 1} {:name "Hi-Dive"}] nil)))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}, :options {:options? true}}
         (mutative/parse-update!-args* nil nil [{:id 1} {:name "Hi-Dive"} {:options? true}] nil))))

(deftest update!-test
  (testing "With PK and map conditions"
    (test/with-venues-reset
      (is (= {:next.jdbc/update-count 1}
             (mutative/update! [:test/postgres :venues] 1 {:name "Hi-Dive"})))
      (is (= (instance/instance :venues {:id         1
                                         :name       "Hi-Dive"
                                         :category   "bar"
                                         :created-at (t/local-date-time "2017-01-01T00:00")
                                         :updated-at (t/local-date-time "2017-01-01T00:00")})
             (select/select-one [:test/postgres :venues] 1)))))
  (testing "With key-value conditions"
    (test/with-venues-reset
      (is (= {:next.jdbc/update-count 1}
             (mutative/update! [:test/postgres :venues] :name "Tempest" {:name "Hi-Dive"})))))
  (testing "with composite PK"
    (is (= {:next.jdbc/update-count 1}
           (mutative/update! [:test/postgres :venues/composite-pk] [1 "Tempest"] {:name "Hi-Dive"})))))

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
               (select/select-one :venues 1)))
        (testing "Should work for 'magic' normalized keys."
          (is (= {:next.jdbc/update-count 1}
                 (-> (select/select-one :venues 1)
                     (assoc :updated-at (t/local-date-time "2021-05-13T04:19:00"))
                     mutative/save!)))
          (is (= (instance/instance :venues {:id         1
                                             :name       "Hi-Dive"
                                             :category   "bar"
                                             :created-at (t/local-date-time "2017-01-01T00:00")
                                             :updated-at (t/local-date-time "2021-05-13T04:19:00")})
                 (select/select-one :venues 1))))))))

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
  (doseq [returning-keys? [true false]
          :let            [insert! (if returning-keys?
                                     mutative/insert-returning-keys!
                                     mutative/insert!)]]
    (testing (if returning-keys? "insert-returning-keys!" "insert!")
      (test/with-venues-reset
        (test/with-default-connection
          (is (= nil
                 (select/select-one :venues 4)))
          (testing "Insert a single row"
            (is (= (if returning-keys?
                     [4]
                     {:next.jdbc/update-count 1})
                   (insert! :venues {:name "Grant & Green", :category "bar"})))
            (is (= (instance/instance :venues {:id         4
                                               :name       "Grant & Green"
                                               :category   "bar"
                                               :created-at (t/local-date-time "2017-01-01T00:00")
                                               :updated-at (t/local-date-time "2017-01-01T00:00")})
                   (select/select-one :venues 4))))

          (testing "Insert multiple rows"
            (is (= (if returning-keys?
                     [5 6]
                     {:next.jdbc/update-count 2})
                   (insert! :venues [{:name "Black Horse London Pub", :category "bar"}
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
            (is (= (if returning-keys?
                     [7]
                     {:next.jdbc/update-count 1})
                   (insert! :venues :name "HiDive SF", :category "bar")))
            (is (= (instance/instance :venues {:id         7
                                               :name       "HiDive SF"
                                               :category   "bar"
                                               :created-at (t/local-date-time "2017-01-01T00:00")
                                               :updated-at (t/local-date-time "2017-01-01T00:00")})
                   (select/select-one :venues :id 7))))

          (testing "Insert with column names"
            (is (= (if returning-keys?
                     [8 9]
                     {:next.jdbc/update-count 2})
                   (insert! "venues" [:name :category] [["The Ramp" "bar"]
                                                                 ["Louie's" "bar"]])))
            (is (= [(instance/instance "venues" {:id 8, :name "The Ramp"})
                    (instance/instance "venues" {:id 9, :name "Louie's"})]
                   (select/select "venues" :id [:> 7] {:select [:id :name], :order-by [[:id :asc]]})))))))))

(deftest insert-returning-keys!-composite-pk-test
  (test/with-venues-reset
    (test/with-default-connection
      (is (= [[4 "Grant & Green"]]
             (mutative/insert-returning-keys! :venues/composite-pk {:name "Grant & Green", :category "bar"}))))))

(deftest delete!-test
  (test/with-default-connection
    (testing "Delete row by PK"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 1}
               (mutative/delete! :venues 1)))
        (is (= []
               (select/select :venues 1)))
        (is (= #{2}
               (select/select-fn-set :id :venues 2))))
      (testing "Composite PK"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 1}
                 (mutative/delete! :venues/composite-pk [1 "Tempest"])))
          (is (= []
                 (select/select :venues :id 1))))))
    (testing "Delete row by key-value conditions"
      (testing "single row"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 1}
                 (mutative/delete! :venues :name "Tempest")))
          (is (= []
                 (select/select :venues :id 1)))))
      (testing "multiple rows"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 2}
                 (mutative/delete! :venues :category "bar")))
          (is (= #{"store"}
                 (select/select-fn-set :category :venues)))))
      (testing "Toucan-style fn-args vector"
        (test/with-venues-reset
          (is (= {:next.jdbc/update-count 2}
                 (mutative/delete! :venues :id [:> 1]))))))
    (testing "Delete row by HoneySQL query"
      (test/with-venues-reset
        (is (= {:next.jdbc/update-count 2}
               (mutative/delete! :venues {:where [:> :id 1]})))))))
