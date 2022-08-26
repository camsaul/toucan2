(ns toucan2.tools.after-update
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.tools.after :as tools.after]
   [toucan2.util :as u]))

(derive :toucan.query-type/update.* ::tools.after/query-type)

(defmacro define-after-update
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(letfn [(after-fn# [~'&query-type ~'&model ~instance-binding]
             ~@body)]
     (u/maybe-derive ~model ::tools.after/model)
     (m/defmethod tools.after/each-row-fn [:toucan.query-type/update.* ~model]
       [query-type# model#]
       (partial after-fn# query-type# model#))))

(s/fdef define-after-update
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
