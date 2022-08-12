(ns toucan2.delete-test)

;; (deftest delete!-test
;;   (test/with-default-connection
;;     (testing "Delete row by PK"
;;       (test/with-venues-reset
;;         (is (= 3
;;                (select/count :venues)))
;;         (is (= 1
;;                (mutative/delete! :venues 1)))
;;         (is (= []
;;                (select/select :venues 1)))
;;         (is (= 2
;;                (select/count :venues)))
;;         (is (= #{2}
;;                (select/select-fn-set :id :venues 2))))
;;       (testing "Composite PK"
;;         (test/with-venues-reset
;;           (is (= 1
;;                  (mutative/delete! :venues/composite-pk [1 "Tempest"])))
;;           (is (= []
;;                  (select/select :venues :id 1))))))
;;     (testing "Delete row by key-value conditions"
;;       (testing "single row"
;;         (test/with-venues-reset
;;           (is (= 1
;;                  (mutative/delete! :venues :name "Tempest")))
;;           (is (= []
;;                  (select/select :venues :id 1)))))
;;       (testing "multiple rows"
;;         (test/with-venues-reset
;;           (is (= 2
;;                  (mutative/delete! :venues :category "bar")))
;;           (is (= #{"store"}
;;                  (select/select-fn-set :category :venues)))))
;;       (testing "Toucan-style fn-args vector"
;;         (test/with-venues-reset
;;           (is (= 2
;;                  (mutative/delete! :venues :id [:> 1]))))))
;;     (testing "Delete row by HoneySQL query"
;;       (test/with-venues-reset
;;         (is (= 2
;;                (mutative/delete! :venues {:where [:> :id 1]})))))))

;; (deftest delete!-custom-honeysql-test
;;   (test/with-default-connection
;;     (testing "Delete row by PK"
;;       (test/with-venues-reset
;;         (is (= ["DELETE FROM venues WHERE id = ?::integer" "1"]
;;                (query/compiled
;;                  (mutative/delete! :venues/custom-honeysql "1"))))
;;         (is (= 1
;;                (mutative/delete! :venues/custom-honeysql "1")))
;;         (is (= []
;;                (select/select :venues 1)))
;;         (is (= #{2}
;;                (select/select-fn-set :id :venues 2)))))
;;     (testing "Delete row by key-value conditions"
;;       (test/with-venues-reset
;;         (is (= 1
;;                (mutative/delete! :venues/custom-honeysql :id "1")))
;;         (is (= []
;;                (select/select :venues :id 1))))
;;       (testing "Toucan-style fn-args vector"
;;         (test/with-venues-reset
;;           (is (= 1
;;                  (mutative/delete! :venues/custom-honeysql :id [:in ["1"]])))
;;           (is (= []
;;                  (select/select :venues :id 1))))))))
