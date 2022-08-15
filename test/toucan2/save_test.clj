(ns toucan2.save-test
  (:require [clojure.test :refer :all]
            [toucan2.save :as save]
            [toucan2.test :as test]
            [toucan2.select :as select]
            [toucan2.instance :as instance]
            [toucan2.execute :as execute])
  (:import java.time.LocalDateTime))

(deftest save-test
  (test/with-discarded-table-changes :venues
    (let [venue (select/select-one ::test/venues 1)]
      (is (= nil
             (instance/changes venue)))
      (testing "Should return updated instance, with value of 'original' reset to what was saved."
        (let [updated (save/save! (assoc venue :name "Hi-Dive"))]
          (is (= {:id         1
                  :name       "Hi-Dive"
                  :category   "bar"
                  :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                 (dissoc updated :updated-at)
                 (dissoc (instance/original updated) :updated-at)))
          (is (= nil
                 (instance/changes updated)))))
      (is (= (instance/instance ::test/venues {:id         1
                                               :name       "Hi-Dive"
                                               :category   "bar"
                                               :created-at (LocalDateTime/parse "2017-01-01T00:00")})
             (dissoc (select/select-one ::test/venues 1) :updated-at))))))

(deftest magic-normalized-keys-test
  (testing "Should work for 'magic' normalized keys."
    (test/with-discarded-table-changes :venues
      (is (= {:id         1
              :name       "Tempest"
              :category   "bar"
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse "2021-05-13T04:19:00")}
             (-> (select/select-one ::test/venues 1)
                 (assoc :updated-at (LocalDateTime/parse "2021-05-13T04:19:00"))
                 save/save!)))
      (is (= (instance/instance ::test/venues {:id         1
                                               :name       "Tempest"
                                               :category   "bar"
                                               :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                               :updated-at (LocalDateTime/parse "2021-05-13T04:19:00")})
             (select/select-one ::test/venues 1))))))

(deftest no-op-test
  (testing "If there are no changes, return object as-is"
    (let [venue (select/select-one ::test/venues 1)]
      (execute/with-call-count [call-count]
        (is (identical? venue
                        (save/save! venue)))
        (is (zero? (call-count)))))))

(deftest no-matching-rows-test
  (testing "If no rows match the PK, save! should throw an Exception."
    (let [venue (assoc (instance/instance ::test/venues {:id 1000})
                       :name "Wow")]
      (is (= {:name "Wow"}
             (instance/changes venue)))
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"Unable to save object: :toucan2.test/venues with primary key \{:id 1000\} does not exist"
           (save/save! venue))))))

;;; TODO
#_(deftest save!-custom-honeysql-test
    (test/with-discarded-table-changes :venues
      (test/with-default-connection
        (let [venue (assoc (select/select-one :venues/custom-honeysql :id "1")
                           :name "Hi-Dive")]
          (is (instance/toucan2-instance? venue))
          (is (= {:name "Hi-Dive"}
                 (instance/changes venue)))
          (is (= ["UPDATE venues SET name = ?, id = ?::integer WHERE id = ?::integer" "Hi-Dive" "1" "1"]
                 (query/compiled (save/save! (assoc venue :id "1")))))
          (is (= {:id         "1"
                  :name       "Hi-Dive"
                  :category   "bar"
                  :created-at (LocalDateTime/parse "2017-01-01T00:00")}
                 (dissoc (save/save! (assoc venue :id "1")) :updated-at)))))))
