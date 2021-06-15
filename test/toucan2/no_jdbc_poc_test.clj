(ns toucan2.no-jdbc-poc-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.core :as toucan2]
            [toucan2.select :as select]))

(m/defmethod toucan2/default-connectable-for-tableable* ::system-properties
  [_ _]
  ::system-properties)

(m/defmethod toucan2/queryable* [::system-properties :default :default]
  [_ _ queryable _]
  queryable)

(m/defmethod toucan2/compile* [::system-properties :default :default]
  [_ _ query _]
  query)

(m/defmethod toucan2/reducible-query* [::system-properties :default :default]
  [connectable tableable queryable options]
  (let [ks (toucan2/compile connectable tableable queryable options)]
    (assert (sequential? ks))
    (assert (every? (some-fn keyword? string?) ks))
    (into {} (for [k ks]
               [(keyword k) (System/getProperty (name k))]))))

(m/defmethod select/parse-select-args* [::system-properties :default]
  [_ _ args _]
  {:query (with-meta args {:type ::system-properties-query}), :options nil})

(deftest select-test
  (is (= {:user.language "en"}
         (toucan2/select ::system-properties :user.language)))
  (is (= {:user.language "en", :line.separator "\n"}
         (toucan2/select ::system-properties :user.language :line.separator))))
