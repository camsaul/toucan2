(ns toucan2.types-test
  (:require
   [clojure.test :refer :all]
   [toucan2.types :as types]))

(derive ::insert-type :toucan.query-type/insert.instances)

(deftest ^:parallel base-query-type-test
  (are [query-type] (= :toucan.query-type/insert.*
                       (types/base-query-type query-type))
    :toucan.query-type/insert.*
    :toucan.query-type/insert.instances
    ::insert-type)
  (are [query-type] (= nil
                       (types/base-query-type query-type))
    nil
    :a
    :toucan.query-type/*
    :toucan.result-type/instances))

(deftest ^:parallel similar-query-type-returning-test
  (are [query-type] (= :toucan.query-type/insert.pks
                       (types/similar-query-type-returning query-type :toucan.result-type/pks))
    :toucan.query-type/insert.*
    :toucan.query-type/insert.instances
    :toucan.query-type/insert.pks
    ::insert-type)
  (are [query-type] (= nil
                       (types/similar-query-type-returning query-type :toucan.result-type/pks))
    nil
    :a
    :toucan.query-type/select.instances ; there is no SELECT returning PKs.
    :toucan.query-type/*
    :toucan.result-type/pks
    :toucan.result-type/instances))
