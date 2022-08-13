(ns toucan2.no-jdbc-poc-test)

;;; TODO

#_(m/defmethod toucan2/default-connectable-for-tableable* ::system-properties
    [_ _]
    ::system-properties)

#_(m/defmethod toucan2/queryable* [::system-properties :default :default]
  [_ _ queryable _]
  queryable)

#_(m/defmethod toucan2/compile* [::system-properties :default :default]
  [_ _ query _]
  query)

#_(m/defmethod toucan2/reducible-query* [::system-properties :default :default]
  [connectable tableable queryable options]
  (let [ks (toucan2/compile connectable tableable queryable options)]
    (assert (sequential? ks))
    (assert (every? (some-fn keyword? string?) ks))
    (into {} (for [k ks]
               [(keyword k) (System/getProperty (name k))]))))

#_(m/defmethod select/parse-select-args* [::system-properties :default]
  [_ _ args _]
  {:query (with-meta args {:type ::system-properties-query}), :options nil})

#_(deftest select-test
  (is (= {:user.language "en"}
         (toucan2/select ::system-properties :user.language)))
  (is (= {:user.language "en", :line.separator "\n"}
         (toucan2/select ::system-properties :user.language :line.separator))))
