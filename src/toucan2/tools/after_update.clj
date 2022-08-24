(ns toucan2.tools.after-update
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.tools.after :as tools.after]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(derive ::update/update ::tools.after/after)
(derive ::after-update ::tools.after/after)

(defmacro define-after-update
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::after-update)
     (m/defmethod tools.after/after [::update/update model#]
       [~'&query-type ~'&model ~instance-binding]
       ~@body)))

(s/fdef define-after-update
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
