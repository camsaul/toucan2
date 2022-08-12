(ns toucan2.insert-test)

;; (deftest parse-insert!-args-test
;;   (testing "single map row"
;;     (is (= {:rows [{:row 1}]}
;;            (mutative/parse-insert!-args* nil nil [{:row 1}] nil)))
;;     (is (= {:rows [{:row 1}], :options {:options? true}}
;;            (mutative/parse-insert!-args* nil nil [{:row 1} {:options? true}] nil))))
;;   (testing "multiple map rows"
;;     (is (= {:rows [{:row 1} {:row 2}]}
;;            (mutative/parse-insert!-args* nil nil [[{:row 1} {:row 2}]] nil)))
;;     (is (= {:rows [{:row 1} {:row 2}], :options {:options? true}}
;;            (mutative/parse-insert!-args* nil nil [[{:row 1} {:row 2}] {:options? true}] nil))))
;;   (testing "kv args"
;;     (is (= {:rows [{:a 1, :b 2, :c 3}]}
;;            (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c 3] nil)))
;;     (is (= {:rows [{:a 1, :b 2, :c 3}], :options {:options? true}}
;;            (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c 3 {:options? true}] nil)))
;;     (is (thrown-with-msg?
;;          clojure.lang.ExceptionInfo
;;          #"Don't know how to interpret insert\! args: \(\) - failed: Insufficient input"
;;          (mutative/parse-insert!-args* nil nil [:a 1, :b 2, :c] nil))))
;;   (testing "columns + vector rows"
;;     (is (= {:rows [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}]}
;;            (mutative/parse-insert!-args* nil nil [[:a :b :c] [[1 2 3] [4 5 6]]] nil)))
;;     (is (= {:rows [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}], :options {:options? true}}
;;            (mutative/parse-insert!-args* nil nil [[:a :b :c] [[1 2 3] [4 5 6]] {:options? true}] nil)))))

;; (deftest insert!-test
;;   (doseq [returning-keys? [true false]
;;           :let            [insert! (if returning-keys?
;;                                      mutative/insert-returning-keys!
;;                                      mutative/insert!)]]
;;     (testing (if returning-keys? "insert-returning-keys!" "insert!")
;;       (test/with-venues-reset
;;         (test/with-default-connection
;;           (is (= nil
;;                  (select/select-one :venues 4)))
;;           (testing "Insert a single row"
;;             (is (= (if returning-keys?
;;                      [4]
;;                      1)
;;                    (insert! :venues {:name "Grant & Green", :category "bar"})))
;;             (is (= (instance/instance :venues {:id         4
;;                                                :name       "Grant & Green"
;;                                                :category   "bar"
;;                                                :created-at (t/local-date-time "2017-01-01T00:00")
;;                                                :updated-at (t/local-date-time "2017-01-01T00:00")})
;;                    (select/select-one :venues 4))))

;;           (testing "Insert multiple rows"
;;             (is (= (if returning-keys?
;;                      [5 6]
;;                      2)
;;                    (insert! :venues [{:name "Black Horse London Pub", :category "bar"}
;;                                      {:name "Nick's Crispy Tacos", :category "bar"}])))
;;             (is (= [(instance/instance :venues {:id         5
;;                                                 :name       "Black Horse London Pub"
;;                                                 :category   "bar"
;;                                                 :created-at (t/local-date-time "2017-01-01T00:00")
;;                                                 :updated-at (t/local-date-time "2017-01-01T00:00")})
;;                     (instance/instance :venues {:id         6
;;                                                 :name       "Nick's Crispy Tacos"
;;                                                 :category   "bar"
;;                                                 :created-at (t/local-date-time "2017-01-01T00:00")
;;                                                 :updated-at (t/local-date-time "2017-01-01T00:00")})]
;;                    (select/select :venues :id [:> 4] {:order-by [[:id :asc]]}))))

;;           (testing "Insert with key/values"
;;             (is (= (if returning-keys?
;;                      [7]
;;                      1)
;;                    (insert! :venues :name "HiDive SF", :category "bar")))
;;             (is (= (instance/instance :venues {:id         7
;;                                                :name       "HiDive SF"
;;                                                :category   "bar"
;;                                                :created-at (t/local-date-time "2017-01-01T00:00")
;;                                                :updated-at (t/local-date-time "2017-01-01T00:00")})
;;                    (select/select-one :venues :id 7))))

;;           (testing "Insert with column names"
;;             (is (= (if returning-keys?
;;                      [8 9]
;;                      2)
;;                    (insert! "venues" [:name :category] [["The Ramp" "bar"]
;;                                                         ["Louie's" "bar"]])))
;;             (is (= [(instance/instance "venues" {:id 8, :name "The Ramp"})
;;                     (instance/instance "venues" {:id 9, :name "Louie's"})]
;;                    (select/select "venues" :id [:> 7] {:select [:id :name], :order-by [[:id :asc]]})))))))))

;; (deftest insert-returning-keys!-composite-pk-test
;;   (test/with-venues-reset
;;     (test/with-default-connection
;;       (is (= [[4 "Grant & Green"]]
;;              (mutative/insert-returning-keys! :venues/composite-pk {:name "Grant & Green", :category "bar"}))))))

;; (deftest insert!-custom-honeysql-test
;;   (test/with-default-connection
;;     (testing "single map row"
;;       (test/with-venues-reset
;;         (is (= 1
;;                (mutative/insert! :venues/custom-honeysql {:id "4", :name "Hi-Dive", :category "bar"})))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one :venues :id 4 {:select [:id :name]})))))
;;     (testing "multiple map rows"
;;       (test/with-venues-reset
;;         (is (= 1
;;                (mutative/insert! :venues/custom-honeysql [{:id "4", :name "Hi-Dive", :category "bar"}])))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one :venues :id 4 {:select [:id :name]})))))
;;     (testing "kv args"
;;       (test/with-venues-reset
;;         (is (= 1
;;                (mutative/insert! :venues/custom-honeysql :id "4", :name "Hi-Dive", :category "bar")))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one :venues :id 4 {:select [:id :name]})))))
;;     (testing "columns + vector rows"
;;       (test/with-venues-reset
;;         (is (= 1
;;                (mutative/insert! :venues/custom-honeysql [:id :name :category] [["4" "Hi-Dive" "bar"]])))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one :venues :id 4 {:select [:id :name]})))))
;;     (testing "returning-keys"
;;       (test/with-venues-reset
;;         (is (= [4]
;;                (mutative/insert-returning-keys! :venues/custom-honeysql [{:id "4", :name "Hi-Dive", :category "bar"}])))))))

;; (deftest insert!-no-changes-no-op-test
;;   (testing "If there are no rows, insert! should no-op and return zero"
;;       (is (= 0
;;     (test/with-venues-reset
;;       (mutative/insert! [:test/postgres :venues] []))))))
