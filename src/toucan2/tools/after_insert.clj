(ns toucan2.tools.after-insert
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.tools.after :as tools.after]
   [toucan2.util :as u]))

(derive :toucan.query-type/insert.* ::tools.after/query-type)

(defmacro define-after-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(letfn [(after-fn# [~'&query-type ~'&model ~instance-binding]
             ~@body)]
     (u/maybe-derive ~model ::tools.after/model)
     (m/defmethod tools.after/each-row-fn [#_query-type :toucan.query-type/insert.*
                                           #_model      ~model]
       [query-type# model#]
       (let [f#      (partial after-fn# query-type# model#)
             next-f# (when ~'next-method
                       (~'next-method query-type# model#))]
         (if next-f#
           (comp next-f# f#)
           f#)))))

(s/fdef define-after-insert
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
