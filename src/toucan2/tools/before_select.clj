(ns toucan2.tools.before-select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment types/keep-me)

(m/defmulti before-select
  "Impl for [[define-before-select]]."
  {:arglists            '([model₁ parsed-args])
   :defmethod-arities   #{2}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod before-select :around :default
  [model parsed-args]
  (u/try-with-error-context ["before select" {::model model}]
    (log/debugf "do before-select for %s" model)
    (let [result (next-method model parsed-args)]
      (log/debugf "[before select] => %s" result)
      result)))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/select.*
                             #_model          ::model
                             #_resolved-query :default]
  [query-type model parsed-args resolved-query]
  (let [parsed-args (before-select model parsed-args)]
    (next-method query-type model parsed-args resolved-query)))

(defmacro define-before-select
  {:style/indent :defn}
  [model [args-binding] & body]
  `(do
     (u/maybe-derive ~model ::model)
     (m/defmethod before-select ~model
       [~'&model ~args-binding]
       (cond->> (do ~@body)
         ~'next-method
         (~'next-method ~'&model)))))

(s/fdef define-before-select
  :args (s/cat :dispatch-value some?
               :bindings       (s/spec (s/cat :args :clojure.core.specs.alpha/binding-form))
               :body           (s/+ any?))
  :ret any?)
