(ns toucan2.no-jdbc-poc-test
  (:require [clojure.test :refer :all]
            [toucan2.core :as toucan2]
            [toucan2.select :as select]))

(toucan2/defmethod toucan2/default-connectable-for-tableable* ::system-properties
  [_ _]
  ::system-properties)

(toucan2/defmethod toucan2/queryable* [::system-properties :default :default]
  [_ _ queryable _]
  queryable)

(toucan2/defmethod toucan2/compile* [::system-properties :default :default]
  [_ _ query _]
  query)

(toucan2/defmethod toucan2/reducible-query* [::system-properties :default :default]
  [connectable tableable queryable options]
  (let [ks (toucan2/compile connectable tableable queryable options)]
    (assert (sequential? ks))
    (assert (every? (some-fn keyword? string?) ks))
    (into {} (for [k ks]
               [(keyword k) (System/getProperty (name k))]))))

(toucan2/defmethod select/parse-select-args* [::system-properties :default]
  [_ _ args _]
  {:query args, :options nil})

(toucan2/defmethod select/compile-select* [::system-properties :default]
  [_ _ args _]
  args)

(deftest select-test
  (is (= {:user.language "en"}
         (toucan2/select ::system-properties :user.language)))
  (is (= {:user.language "en", :line.separator "\n"}
         (toucan2/select ::system-properties :user.language :line.separator))))
