(ns bluejdbc.experimental-test
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.experimental :as experimental]
            [bluejdbc.instance :as instance]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(comment experimental/keep-me)

(derive :venues/return-instances :bluejdbc.experimental/insert-return-instances)

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
