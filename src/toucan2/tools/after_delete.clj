(ns toucan2.tools.after-delete
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.tools.before-delete :as before-delete]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment types/keep-me)

(m/defmulti after-delete
  "Underlying method implemented when using [[define-after-delete]]. You *probably* shouldn't be adding implementations
  to this method directly, unless you know what you are doing!"
  {:arglists            '([model₁ instance])
   :defmethod-arities   #{2}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod after-delete :around :default
  [model instance]
  (log/tracef "Do after-delete for %s %s" model instance)
  (next-method model instance))

(defn- matching-rows
  "Select and realize the rows matching the args passed to `delete!`, before they are deleted."
  [model parsed-args resolved-query]
  (let [rows (volatile! [])]
    (before-delete/-do-for-matching-rows! #(vswap! rows conj %) model parsed-args resolved-query)
    @rows))

(m/defmethod pipeline/transduce-query [#_query-type     :toucan.query-type/delete.*
                                       #_model          ::after-delete
                                       #_resolved-query :default]
  "Do a recursive SELECT query with the args passed to `delete!`, run the `next-method` to delete the matching rows, and
  then apply [[after-delete]] to each row that was deleted. This is all done inside of a transaction."
  [rf query-type model parsed-args resolved-query]
  (conn/with-transaction [_conn nil {:nested-transaction-rule :ignore}]
    (let [rows   (matching-rows model parsed-args resolved-query)
          result (next-method rf query-type model parsed-args resolved-query)]
      (doseq [row rows]
        (after-delete model row))
      result)))

;;; A model with both before-delete and after-delete runs its before-delete methods first, and the after-delete method
;;; above as the `next-method`.
(m/prefer-method! #'pipeline/transduce-query
                  [:toucan.query-type/delete.* ::before-delete/before-delete :default]
                  [:toucan.query-type/delete.* ::after-delete :default])

(defn ^:no-doc -after-delete-impl
  "Implementation of [[define-after-delete]]; don't call this directly."
  [next-method model instance f]
  ;; if `f` didn't return anything, just use the original instance again.
  (let [result (or (f model instance)
                   instance)]
    (if next-method
      (next-method model result)
      result)))

(defmacro define-after-delete
  "Define a method that will be called for every row deleted by [[toucan2.delete/delete!]], with the row as it was right
  before the `DELETE`, as the model's select returns it, so default fields and after-select apply. Its result is
  ignored, other than being passed to the next method, e.g. a parent model's after-delete. The select, the delete, and
  the after-delete methods are done in one transaction, so an after-delete method can keep other rows up to date, or
  throw to roll back the delete. If code inside a transaction that was already open catches the exception, though,
  that transaction keeps the `DELETE`.

  After-delete selects the rows matching the `delete!` conditions before the `DELETE` and holds all of them in memory
  until it is done, so it can be quite expensive for deletes of many rows! Be sure you *really* need after-delete
  behavior before opting in to it. A model's before-delete methods run first, and after-delete then selects the rows
  again, so it sees their changes. Deletes with raw SQL, or with Honey SQL clauses other than `:delete`,
  `:delete-from`, `:from`, and `:where`, throw, as their rows can't be selected first.

  To skip after-delete behavior, you can always use the model's raw table name directly, e.g.

  ```clj
  (t2/delete! (t2/table-name :models/user) ...)
  ```"
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(do
     (u/unparent-descendants ~model ::after-delete)
     (u/maybe-derive ~model ::after-delete)
     (m/defmethod after-delete ~model
       [model# instance#]
       (-after-delete-impl ~'next-method
                           model#
                           instance#
                           (fn [~'&model ~instance-binding]
                             ~@body)))))

(s/fdef define-after-delete
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
