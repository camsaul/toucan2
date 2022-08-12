(ns toucan2.save-test)

;; (deftest save!-test
;;   (test/with-venues-reset
;;     (test/with-default-connection
;;       (let [venue (select/select-one :venues 1)]
;;         (is (= nil
;;                (instance/changes venue)))
;;         (testing "Should return updated instance, with value of 'original' reset to what was saved."
;;           (let [updated (mutative/save! (assoc venue :name "Hi-Dive"))]
;;             (is (= {:id         1
;;                     :name       "Hi-Dive"
;;                     :category   "bar"
;;                     :created-at (t/local-date-time "2017-01-01T00:00")}
;;                    (dissoc updated :updated-at)
;;                    (dissoc (instance/original updated) :updated-at)))
;;             (is (= nil
;;                    (instance/changes updated)))))
;;         (is (= (instance/instance :venues {:id         1
;;                                            :name       "Hi-Dive"
;;                                            :category   "bar"
;;                                            :created-at (t/local-date-time "2017-01-01T00:00")})
;;                (dissoc (select/select-one :venues 1) :updated-at)))
;;         (testing "Should work for 'magic' normalized keys."
;;           (is (= {:id         1
;;                   :name       "Hi-Dive"
;;                   :category   "bar"
;;                   :created-at (t/local-date-time "2017-01-01T00:00")
;;                   :updated-at (t/local-date-time "2021-05-13T04:19:00")}
;;                  (-> (select/select-one :venues 1)
;;                      (assoc :updated-at (t/local-date-time "2021-05-13T04:19:00"))
;;                      mutative/save!)))
;;           (is (= (instance/instance :venues {:id         1
;;                                              :name       "Hi-Dive"
;;                                              :category   "bar"
;;                                              :created-at (t/local-date-time "2017-01-01T00:00")
;;                                              :updated-at (t/local-date-time "2021-05-13T04:19:00")})
;;                  (select/select-one :venues 1))))
;;         (testing "If there are no changes, return object as-is"
;;           (let [venue (select/select-one :venues 1)]
;;             (query/with-call-count [call-count]
;;               (is (identical? venue
;;                               (mutative/save! venue)))
;;               (is (zero? (call-count))))))
;;         (testing "If no rows match the PK, save! should throw an Exception."
;;           (let [venue (assoc (instance/instance :venues {:id 1000})
;;                              :name "Wow")]
;;             (is (= {:name "Wow"}
;;                    (instance/changes venue)))
;;             (is (thrown-with-msg?
;;                  clojure.lang.ExceptionInfo
;;                  #"Unable to save object: :venues with primary key \{:id 1000\} does not exist"
;;                  (mutative/save! venue)))))))))

;; (deftest save!-custom-honeysql-test
;;   (test/with-venues-reset
;;     (test/with-default-connection
;;       (let [venue (assoc (select/select-one :venues/custom-honeysql :id "1")
;;                          :name "Hi-Dive")]
;;         (is (instance/toucan2-instance? venue))
;;         (is (= {:name "Hi-Dive"}
;;                (instance/changes venue)))
;;         (is (= ["UPDATE venues SET name = ?, id = ?::integer WHERE id = ?::integer" "Hi-Dive" "1" "1"]
;;                (query/compiled (mutative/save! (assoc venue :id "1")))))
;;         (is (= {:id         "1"
;;                 :name       "Hi-Dive"
;;                 :category   "bar"
;;                 :created-at (t/local-date-time "2017-01-01T00:00")}
;;                (dissoc (mutative/save! (assoc venue :id "1")) :updated-at)))))))
