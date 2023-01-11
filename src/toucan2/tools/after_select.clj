(ns toucan2.tools.after-select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.tools.simple-out-transform :as tools.simple-out-transform]
   [toucan2.util :as u]))

(m/defmulti after-select
  {:arglists '([instance])}
  u/dispatch-on-first-arg)

(tools.simple-out-transform/define-out-transform [:toucan.query-type/select.instances ::after-select]
  [instance]
  ;; don't do after-select if this select is a result of doing something like insert-returning instances
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
