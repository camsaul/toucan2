(ns toucan2.no-jdbc-poc-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.query :as query]
   [toucan2.select :as select]))

(m/defmethod query/build [:default ::system-properties :default]
  [_query-type _model {:keys [query]}]
  (let [query (if (sequential? query)
                query
                [query])]
    (with-meta query {:type ::system-properties})))

(m/defmethod execute/reduce-uncompiled-query [:default ::system-properties]
  [_connectable _model ks rf init]
  (assert (sequential? ks))
  (assert (every? (some-fn keyword? string?) ks))
  (reduce rf init [(into {} (for [k ks]
                              [(keyword k) (System/getProperty (name k))]))]))

(deftest select-test
  ;; TODO FIXME -- these are busted because [[toucan2.tools.compile/build]] hooks into the compilation step, and we are
  ;; skipping that step.
  ;;
  ;; (is (= [:user.language]
  ;;        (tools.compile/build
  ;;          (select/select ::system-properties :user.language))))
  ;; (is (= [:user.language :line.separator]
  ;;        (tools.compile/build
  ;;          (select/select ::system-properties :user.language :line.separator))))
  (is (= [{:user.language "en"}]
         (select/select ::system-properties :user.language)))
  (is (= [{:user.language "en", :line.separator "\n"}]
         (select/select ::system-properties [:user.language :line.separator])))
  (is (= {:user.language "en", :line.separator "\n"}
         (select/select-one ::system-properties [:user.language :line.separator])))
  (is (= #{"en"}
         (select/select-fn-set :user.language ::system-properties [:user.language :line.separator])))
  (is (= "en"
         (select/select-one-fn :user.language ::system-properties [:user.language :line.separator]))))
