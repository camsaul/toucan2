(ns toucan2.experimental-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]
            [toucan2.connectable.current :as conn.current]
            [toucan2.experimental :as experimental]
            [toucan2.instance :as instance]
            [toucan2.mutative :as mutative]
            [toucan2.test :as test]))

(use-fixtures :once test/do-with-test-data)

(comment experimental/keep-me)

(derive :venues/return-instances :toucan2.experimental/insert-return-instances)

(m/defmethod conn.current/default-connectable-for-tableable* :venues/return-instances
  [_ _]
  :test/postgres)

(deftest insert!-return-instances-test
  (test/with-venues-reset
    (is (= [(instance/instance
             :venues/return-instances
             {:id         4
              :name       "Grant & Green"
              :category   "bar"
              :created-at (t/local-date-time "2017-01-01T00:00")
              :updated-at (t/local-date-time "2017-01-01T00:00")})]
           (mutative/insert! :venues/return-instances {:name "Grant & Green", :category "bar"})))))
