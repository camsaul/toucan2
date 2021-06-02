(ns bluejdbc.no-jdbc-poc-test
  (:require [bluejdbc.core :as bluejdbc]
            [bluejdbc.select :as select]
            [clojure.test :refer :all]))

(bluejdbc/defmethod bluejdbc/default-connectable-for-tableable* ::system-properties
  [_ _]
  ::system-properties)

(bluejdbc/defmethod bluejdbc/queryable* [::system-properties :default :default]
  [_ _ queryable _]
  queryable)

(bluejdbc/defmethod bluejdbc/compile* [::system-properties :default :default]
  [_ _ query _]
  query)

(bluejdbc/defmethod bluejdbc/reducible-query* [::system-properties :default :default]
  [connectable tableable queryable options]
  (let [ks (bluejdbc/compile connectable tableable queryable options)]
    (assert (sequential? ks))
    (assert (every? (some-fn keyword? string?) ks))
    (into {} (for [k ks]
               [(keyword k) (System/getProperty (name k))]))))

(bluejdbc/defmethod select/parse-select-args* [::system-properties :default]
  [_ _ args _]
  {:query args, :options nil})

(bluejdbc/defmethod select/compile-select* [::system-properties :default]
  [_ _ args _]
  args)

(deftest select-test
  (is (= {:user.language "en"}
         (bluejdbc/select ::system-properties :user.language)))
  (is (= {:user.language "en", :line.separator "\n"}
         (bluejdbc/select ::system-properties :user.language :line.separator))))
