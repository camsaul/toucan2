(ns toucan2.tools.after-update
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.tools.after :as tools.after]))

(derive :toucan.query-type/update.* ::tools.after/query-type)

(defmacro define-after-update
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(tools.after/define-after :toucan.query-type/update.*
     ~model
     [~instance-binding]
     ~@body))

(s/fdef define-after-update
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
