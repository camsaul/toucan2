(ns toucan2.tools.before-delete
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti before-delete
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (u/with-debug-result (list `before-delete model instance)
    (next-method model instance)))

(m/defmethod pipeline/transduce-with-model :before [#_query-type :toucan.query-type/delete.*
                                                    #_model      ::before-delete]
  [_rf _query-type model parsed-args]
  (conn/with-transaction [_conn
                          (or conn/*current-connectable*
                              (model/default-connectable model))
                          {:nested-transaction-rule :ignore}]
    ;; select and transduce the matching rows and run their [[before-delete]] methods
    (pipeline/transduce-with-model
     ((map (fn [row]
             (before-delete model row)))
      (constantly nil))
     :toucan.query-type/select.instances
     model
     parsed-args)
    ;; cool, now we can proceed
    parsed-args))

(defn ^:no-doc before-delete-impl [next-method model instance f]
  (let [result (or (f model instance)
                   instance)]
    (if next-method
      (next-method model result)
      result)))

(defmacro define-before-delete
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(do
     (u/maybe-derive ~model ::before-delete)
     (m/defmethod before-delete ~model
       [model# instance#]
       (before-delete-impl ~'next-method
                           model#
                           instance#
                           (fn [~'&model ~instance-binding]
                             ~@body)))))

(s/fdef define-before-delete
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
