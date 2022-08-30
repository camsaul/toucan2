(ns toucan2.no-jdbc-poc-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.select :as select]))

(set! *warn-on-reflection* true)

(m/defmethod pipeline/transduce-with-model* [:default ::system-properties]
  [rf _query-type _model {:keys [queryable kv-args]}]
  (let [ks (into (if (keyword? queryable) [queryable] [])
                 (mapcat identity)
                 kv-args)]
    (assert (sequential? ks))
    (assert (every? (some-fn keyword? string?) ks))
    (transduce
     identity
     rf
     [(into {} (for [k ks]
                 [(keyword k) (System/getProperty (name k))]))])))

(deftest select-test
  (is (= [{:user.language "en"}]
         (select/select ::system-properties :user.language)))
  (is (= [{:user.language "en", :line.separator "\n"}]
         (select/select ::system-properties :user.language :line.separator)))
  (is (= {:user.language "en", :line.separator "\n"}
         (select/select-one ::system-properties :user.language :line.separator)))
  (is (= #{"en"}
         (select/select-fn-set :user.language ::system-properties :user.language :line.separator)))
  (is (= "en"
         (select/select-one-fn :user.language ::system-properties :user.language :line.separator))))
