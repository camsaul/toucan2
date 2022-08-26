(ns toucan2.tools.after-select
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.pipeline :as pipeline]))

(defmacro define-after-select
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(pipeline/define-out-transform [:toucan.query-type/select.instances ~model]
     [~instance-binding]
     ~@body))

(s/fdef define-after-select
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
