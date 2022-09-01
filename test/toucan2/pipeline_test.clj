(ns toucan2.pipeline-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]))

(derive ::insert-type :toucan.query-type/insert.instances)

(deftest ^:parallel base-query-type-test
  (are [query-type] (= :toucan.query-type/insert.*
                       (pipeline/base-query-type query-type))
    :toucan.query-type/insert.*
    :toucan.query-type/insert.instances
    ::insert-type)
  (are [query-type] (= nil
                       (pipeline/base-query-type query-type))
    nil
    :a
    :toucan.query-type/*
    :toucan.result-type/instances))

(deftest ^:parallel similar-query-type-returning-test
  (are [query-type] (= :toucan.query-type/insert.pks
                       (pipeline/similar-query-type-returning query-type :toucan.result-type/pks))
    :toucan.query-type/insert.*
    :toucan.query-type/insert.instances
    :toucan.query-type/insert.pks
    ::insert-type)
  (are [query-type] (= nil
                       (pipeline/similar-query-type-returning query-type :toucan.result-type/pks))
    nil
    :a
    :toucan.query-type/select.instances ; there is no SELECT returning PKs.
    :toucan.query-type/*
    :toucan.result-type/pks
    :toucan.result-type/instances))

(deftest ^:parallel build-test
  (is (= {:where [:= :a 1]}
         (pipeline/build :toucan.query-type/* nil {:kv-args {:a 1}} {}))))

(m/defmethod model/primary-keys ::model-with-non-id-pk
  [_model]
  [:uuid])

(deftest ^:parallel build-query-for-int-test
  (testing "Raw integer PK as query"
    (is (= {:where [:= :id 1]}
           (pipeline/build :toucan.query-type/* nil {} 1)
           (pipeline/build :toucan.query-type/* nil {:kv-args {:toucan/pk 1}} {}))))
  (testing "custom non-:id PK"
    (is (= {:where [:= :uuid 1]}
           (pipeline/build :toucan.query-type/* ::model-with-non-id-pk {} 1)
           (pipeline/build :toucan.query-type/* ::model-with-non-id-pk {:kv-args {:toucan/pk 1}} {})))))

(deftest ^:parallel plain-sql-query-test
  (doseq [query ["SELECT *"
                 ["SELECT *"]]]
    (testing (pr-str query)
      (is (= ["SELECT *"]
             (pipeline/build :toucan.query-type/* nil {} query)))
      (testing "disallow kv-args"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"key-value args are not supported for plain SQL queries"
             (pipeline/build :toucan.query-type/* nil {:kv-args {:toucan/pk 1}} query)))))))
