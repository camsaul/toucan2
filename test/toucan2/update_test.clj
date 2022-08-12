(ns toucan2.update-test
  (:require [toucan2.update :as update]
            [clojure.test :refer :all]
            [toucan2.test :as test]
            [methodical.core :as m]
            [toucan2.model :as model]
            [toucan2.select :as select]
            [toucan2.instance :as instance])
  (:import java.time.LocalDateTime))

(deftest ^:parallel parse-update-args-test
  (is (= {:changes {:a 1}, :conditions {:toucan2/pk 1}}
         (update/parse-args nil [1 {:a 1}])))
  (is (= {:conditions {:id 1}, :changes {:a 1}}
         (update/parse-args nil [:id 1 {:a 1}])))
  (testing "composite PK"
    (is (= {:changes {:a 1}, :conditions {:toucan2/pk [1 2]}}
           (update/parse-args nil [[1 2] {:a 1}]))))
  (testing "key-value conditions"
    (is (= {:conditions {:name "Cam", :toucan2/pk 1}, :changes {:a 1}}
           (update/parse-args nil [1 :name "Cam" {:a 1}]))))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}}
         (update/parse-args nil [{:id 1} {:name "Hi-Dive"}]))))

(deftest pk-and-map-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues 1 {:name "Hi-Dive"})))
    (is (= (instance/instance ::test/venues {:id         1
                                             :name       "Hi-Dive"
                                             :category   "bar"
                                             :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                             :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           (select/select-one ::test/venues 1)))))

(deftest key-value-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::test/venues :name "Tempest" {:name "Hi-Dive"})))))

(derive ::venues.composite-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.composite-pk
  [_model]
  [:id :name])

(deftest composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (update/update! ::venues.composite-pk [1 "Tempest"] {:name "Hi-Dive"})))))

;; (m/defmethod honeysql.compile/to-sql* [:default ::venues.custom-honeysql :id String]
;;   [_ _ _ v _]
;;   (assert (string? v) (format "V should be a string, got %s" (pr-str v)))
;;   ["?::integer" v])

;; (derive ::venues.custom-honeysql-composite-pk ::venues.composite-pk)
;; (derive ::venues.custom-honeysql-composite-pk ::venues.custom-honeysql)

;; (deftest update!-custom-honeysql-test
;;   (testing "custom HoneySQL for PK"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql "1" {:name "Hi-Dive"})))
;;       (is (= (instance/instance ::venues.custom-honeysql {:id 1, :name "Hi-Dive"})
;;              (select/select-one ::venues.custom-honeysql "1" {:select [:id :name]}))))
;;     (testing "composite PK"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (update/update! ::venues.custom-honeysql-composite-pk ["1" "Tempest"] {:name "Hi-Dive"}))))))
;;   (testing "custom HoneySQL for key-value conditions"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql :id "1" {:name "Hi-Dive"})))))
;;   (testing "custom HoneySQL for changes"
;;     (test/with-discarded-table-changes :venues
;;       (is (= 1
;;              (update/update! ::venues.custom-honeysql :id "1" {:id "100"})))
;;       (is (= (instance/instance ::venues.custom-honeysql {:id 100, :name "Tempest"})
;;              (select/select-one ::venues.custom-honeysql "100" {:select [:id :name]}))))))

(deftest update!-no-changes-no-op-test
  (testing "If there are no changes, update! should no-op and return zero"
    (test/with-discarded-table-changes :venues
      (is (= 0
             (update/update! ::test/venues 1 {})
             (update/update! ::test/venues {}))))))
