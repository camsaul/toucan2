(ns toucan2.tools.after-select
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.pipeline :as pipeline]))

(defmacro define-after-select
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(pipeline/define-out-transform [:toucan.query-type/select.instances ~model]
     [instance#]
     ;; don't do after-select if this select is a result of doing something like insert-returning instances
     (if (isa? ~'&query-type :toucan2.pipeline/select.instances-from-pks)
       instance#
       (let [~instance-binding instance#]
         ~@body))))

(s/fdef define-after-select
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
