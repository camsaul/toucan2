(ns toucan2.tools.before-delete
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti before-delete
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (u/with-debug-result (list `before-delete model instance)
    (next-method model instance)))

(deftype ^:no-doc ReducibleBeforeDelete [model parsed-args reducible-delete]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (conn/with-transaction [_conn (model/deferred-current-connectable model)]
      (transduce
       (map (fn [row]
              (before-delete model row)))
       (constantly nil)
       nil
       (op/reducible-returning-instances* :toucan2.select/select model parsed-args))
      (reduce rf init reducible-delete)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleBeforeDelete model parsed-args reducible-delete)))

(m/defmethod op/reducible-update* :around [::delete/delete ::before-delete]
  [query-type model parsed-args]
  (let [reducible-delete (next-method query-type model parsed-args)]
    (->ReducibleBeforeDelete model parsed-args reducible-delete)))

(defmacro define-before-delete
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-delete)
     (m/defmethod before-delete model#
       [~'&model ~instance-binding]
       ~@body)))

(s/fdef define-before-delete
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
