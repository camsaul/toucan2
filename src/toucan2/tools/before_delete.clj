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
  "Underlying method implemented when using [[define-before-delete]]. You *probably* shouldn't be adding implementations
  to this method directly, unless you know what you are doing!"
  {:arglists            '([model₁ instance])
   :defmethod-arities   #{2}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (log/tracef "Do before-delete for %s %s" model instance)
  (next-method model instance))

(defn- select-query
  "The query that selects the rows a `delete!` with `resolved-query` deletes: a Honey SQL `DELETE` map selects from the
  table it deletes from, keeping its alias (see https://github.com/camsaul/toucan2/issues/203). Throws for raw SQL and
  for maps with other clauses, e.g. `:join` or `:limit`, as the select can't be relied on to match the deleted rows."
  [resolved-query]
  (let [unsupported (fn [reason]
                      (throw (ex-info (str "Can't select the rows this delete deletes, which is needed to call their"
                                           " before-delete and after-delete methods: " reason ". To skip them, delete"
                                           " from the model's table name instead.")
                                      {:query resolved-query})))]
    (cond
      (map? resolved-query)
      (do
        (when-let [ks (not-empty (remove #{:delete :delete-from :from :where} (keys resolved-query)))]
          (unsupported (str "unsupported clauses " (vec ks))))
        (when (next (:from resolved-query))
          (unsupported "more than one table in :from"))
        (cond-> (dissoc resolved-query :delete)
          (contains? resolved-query :delete-from) (-> (dissoc :delete-from)
                                                      (assoc :from [(:delete-from resolved-query)]))))

      (or (string? resolved-query) (sequential? resolved-query))
      (unsupported "raw SQL")

      :else
      resolved-query)))

(defn ^:no-doc -do-for-matching-rows!
  "Select the rows a `delete!` with `parsed-args` and `resolved-query` deletes, and call `f` on each of them. A query
  that is only compiled, e.g. with [[toucan2.tools.compile/compile]], calls `f` on none. Used by before-delete and
  after-delete; don't call this directly."
  [f model parsed-args resolved-query]
  (pipeline/transduce-query
   ((map (fn [row]
           ;; this is another case where we don't NEED to fully realize the rows but it's a big hassle for people
           ;; to use this if we don't. Let's be nice and realize things for people.
           (f (realize/realize row))))
    (constantly nil))
   :toucan.query-type/select.instances
   model
   parsed-args
   (select-query resolved-query)))

(m/defmethod pipeline/transduce-query [#_query-type     :toucan.query-type/delete.*
                                       #_model          ::before-delete
                                       #_resolved-query :default]
  "Do a recursive SELECT query with the args passed to `delete!`; apply [[before-delete]] to all matching rows. Then call
  the `next-method`. This is all done inside of a transaction."
  [rf query-type model parsed-args resolved-query]
  (conn/with-transaction [_conn nil {:nested-transaction-rule :ignore}]
    (-do-for-matching-rows! #(before-delete model %) model parsed-args resolved-query)
    (next-method rf query-type model parsed-args resolved-query)))

(defn ^:no-doc -before-delete-impl
  "Implementation of [[define-before-delete]]; don't call this directly."
  [next-method model instance f]
  ;; if `f` didn't return anything, just use the original instance again.
  (let [result (or (f model instance)
                   instance)]
    (if next-method
      (next-method model result)
      result)))

(defmacro define-before-delete
  "Define a method that will be called for every instance that is about to be deleted. The results of this before-delete
  method are ultimately ignored, but the entire operation (both the original delete and the recursive select) are done
  in a transaction, so you can use before-delete to enforce preconditions and abort deletes when they fail, or do
  something for side effects.

  Before-delete is implemented by first selecting all the rows matching the [[toucan2.delete/delete!]] conditions and
  then transducing those rows and calling the [[before-delete]] method this macro defines on each row. Because
  before-delete has to fetch every instance matching the condition, defining a before-delete method can be quite
  expensive! For example, a `delete!` operation that deletes a million rows would normally be a single database call;
  with before-delete in place, it would have to fetch and transduce all million rows and apply [[before-delete]] to each
  of them *before* even getting to the `DELETE` operation! So be sure you *really* need before-delete behavior before
  opting in to it. Deletes with raw SQL, or with Honey SQL clauses other than `:delete`, `:delete-from`, `:from`, and
  `:where`, throw, as their rows can't be selected first.

  To skip before-delete behavior, you can always use the model's raw table name directly, e.g.

  ```clj
  (t2/delete! (t2/table-name :models/user) ...)
  ```

  This might be wise when deleting a large number of rows."
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(do
     (u/unparent-descendants ~model ::before-delete)
     (u/maybe-derive ~model ::before-delete)
     (m/defmethod before-delete ~model
       [model# instance#]
       (-before-delete-impl ~'next-method
                            model#
                            instance#
                            (fn [~'&model ~instance-binding]
                              ~@body)))))

(s/fdef define-before-delete
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
