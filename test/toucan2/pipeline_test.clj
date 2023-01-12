(ns toucan2.pipeline-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]))

(set! *warn-on-reflection* true)

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
             (re-pattern (java.util.regex.Pattern/quote "key-value args are not supported for [query & args]"))
             (pipeline/build :toucan.query-type/* nil {:kv-args {:toucan/pk 1}} query)))))))
