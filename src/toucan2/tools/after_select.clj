(ns toucan2.tools.after-select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.tools.simple-out-transform :as tools.simple-out-transform]
   [toucan2.util :as u]))

(m/defmulti after-select
  {:arglists '([instance]), :defmethod-arities #{1}}
  u/dispatch-on-first-arg)

;;; Do after-select for anything returning instances, not just SELECT. [[toucan2.insert/insert-returning-instances!]]
;;; should do after-select as well.
(tools.simple-out-transform/define-out-transform [:toucan.result-type/instances ::after-select]
  [instance]
  (if (isa? &query-type :toucan2.pipeline/select.instances-from-pks)
    instance
    (after-select instance)))

(defmacro define-after-select
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(do
     (u/maybe-derive ~model ::after-select)
     (m/defmethod after-select ~model
       [instance#]
       (let [~instance-binding (cond-> instance#
                                 ~'next-method ~'next-method)]
         ~@body))))

(s/fdef define-after-select
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)

;;; `after-select` should be done before [[toucan2.tools.after-update]] and [[toucan2.tools.after-insert]]
(m/prefer-method! #'pipeline/results-transform
                  [:toucan.result-type/instances ::after-select]
                  [:toucan.result-type/instances :toucan2.tools.after/model])
