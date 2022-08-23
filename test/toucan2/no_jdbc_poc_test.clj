(ns toucan2.no-jdbc-poc-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.query :as query]
   [toucan2.select :as select]))

(m/defmethod query/build [:default :default ::system-properties]
  [_query-type _model {:keys [queryable]}]
  queryable)

(m/defmethod execute/reduce-uncompiled-query [:default ::system-properties]
  [_connectable _model ks rf init]
  (assert (sequential? ks))
  (assert (every? (some-fn keyword? string?) ks))
  (reduce rf init [(into {} (for [k ks]
                              [(keyword k) (System/getProperty (name k))]))]))

(m/defmethod query/parse-args [::select/select ::system-properties]
  [_query-type _model unparsed-args]
  {:queryable (with-meta (vec unparsed-args) {:type ::system-properties})})

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
