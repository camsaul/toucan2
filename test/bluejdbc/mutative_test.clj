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
         (mutative/parse-update!-args [1 {:a 1}])))
  (is (= {:conditions {:id 1}, :changes {:a 1}}
         (mutative/parse-update!-args [:id 1 {:a 1}])))
  (is (= {:id 1, :conditions {:name "Cam"}, :changes {:a 1}}
         (mutative/parse-update!-args [1 :name "Cam" {:a 1}])))
  (is (= {:changes {:name "Hi-Dive"}, :conditions {:id 1}}
         (mutative/parse-update!-args [{:id 1} {:name "Hi-Dive"}]))))

(deftest update!-test
  (test/with-venues-reset
    (is (= [{:next.jdbc/update-count 1}]
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
        (is (= [{:next.jdbc/update-count 1}]
               (mutative/save! (assoc venue :name "Hi-Dive"))))
        (is (= (instance/instance :venues {:id         1
                                           :name       "Hi-Dive"
                                           :category   "bar"
                                           :created-at (t/local-date-time "2017-01-01T00:00")
                                           :updated-at (t/local-date-time "2017-01-01T00:00")})
               (select/select-one :venues 1)))))))
