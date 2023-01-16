(ns toucan2.tools.before-delete
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment types/keep-me)

(m/defmulti before-delete
  {:arglists            '([model₁ instance])
   :defmethod-arities   #{2}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (log/tracef :compile "Do before-delete for %s %s" model instance)
  (next-method model instance))

(defn- do-before-delete-for-matching-rows!
  "Select and transduce the matching rows and run their [[before-delete]] methods."
  [model parsed-args resolved-query]
  (pipeline/transduce-query
   ((map (fn [row]
           ;; this is another case where we don't NEED to fully realize the rows but it's a big hassle for people
           ;; to use this if we don't. Let's be nice and realize things for people.
           (before-delete model (realize/realize row))))
    (constantly nil))
   :toucan.query-type/select.instances
   model
   parsed-args
   resolved-query))

(m/defmethod pipeline/transduce-query [#_query-type     :toucan.query-type/delete.*
                                       #_model          ::before-delete
                                       #_resolved-query :default]
  "Do a recursive SELECT query with the args passed to `delete!`; apply [[before-delete]] to all matching rows. Then call
  the `next-method`. This is all done inside of a transaction."
  [rf query-type model parsed-args resolved-query]
  (conn/with-transaction [_conn nil {:nested-transaction-rule :ignore}]
    (do-before-delete-for-matching-rows! model parsed-args resolved-query)
    (next-method rf query-type model parsed-args resolved-query)))

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
