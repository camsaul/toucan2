(ns toucan2.tools.after-insert
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.tools.after :as tools.after]))

(derive :toucan.query-type/insert.* ::tools.after/query-type)

(defmacro define-after-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(tools.after/define-after :toucan.query-type/insert.*
     ~model
     [~instance-binding]
     ~@body))

(s/fdef define-after-insert
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
