(ns toucan2.tools.helpers
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.tools.transformed :as transformed]
   [toucan2.util :as u]))

;;;; [[define-before-select]]

(defn ^:no-doc do-before-select
  "Impl for [[define-before-select]]. Don't call this directly."
  [model thunk]
  (u/try-with-error-context ["before select" {::model model}]
    (u/with-debug-result ["%s %s" `define-before-select model]
      (thunk))))

(defmacro define-before-select
  {:style/indent :defn, :arglists '([model [args-binding] & body]
                                    [[model query-class] [args-binding] & body])}
  [dispatch-value [args-binding] & body]
  (let [[model query-class] (if (vector? dispatch-value)
                              dispatch-value
                              [dispatch-value clojure.lang.IPersistentMap])]
    `(m/defmethod query/build :before [::select/select ~model ~query-class]
       [~'&query-type ~'&model ~args-binding]
       (do-before-select ~'&model (^:once fn* [] ~@body)))))


;;;; [[define-default-fields]]

(m/defmulti default-fields
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod query/build :before [::select/select ::default-fields clojure.lang.IPersistentMap]
  [_query-type model args]
  (u/with-debug-result ["add default fields for %s" model]
    (update args :columns (fn [columns]
                            (or (not-empty columns)
                                (default-fields model))))))

(defmacro define-default-fields {:style/indent :defn} [model & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))


;;;; [[define-before-delete]], [[define-after-delete]]

(m/defmulti before-delete
  {:arglists '([model instance])}
  u/dispatch-on-first-arg)

(m/defmethod before-delete :around :default
  [model instance]
  (u/with-debug-result [(list `before-delete model instance)]
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

;; TODO
#_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
    (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
      [~'&~'&model _ ~'&options]
      ~@body))

;;;; [[deftransforms]]

;;; TODO -- move this to [[toucan2.tools.transformed]]
(defmacro deftransforms
  "Define type transforms to use for a specific model. `transforms` should be a map of

  ```clj
  {:column-name {:in <fn>, :out <fn>}}
  ```

  `:in` transforms are applied to values going over the wire to the database; these generally only applied to values
  passed at or near the top level to various functions; don't expect Toucan 2 to parse your SQL to find out which
  parameter corresponds to what in order to apply transforms or to apply transforms inside JOINS in hand-written
  HoneySQL. That said, unless you're doing something weird your transforms should generally get applied.

  `:out` transforms are applied to values coming out of the database; since nothing weird really happens there this is
  done consistently.

  Transform functions for either case are skipped for `nil` values.

  Example:

  ```clj
  (deftransforms :models/user
    {:type {:in name, :out keyword}})
  ```

  You can also define transforms independently, and derive a model from them:

  ```clj
  (deftransforms ::type-keyword
    {:type {:in name, :out keyword}})

  (derive :models/user ::type-keyword)
  (derive :models/user ::some-other-transform)
  ```

  Don't derive a model from multiple [[deftransforms]] for the same key in the same direction.

  When multiple transforms match a given model they are combined into a single map of transforms with `merge-with
  merge`. If multiple transforms match a given column in a given direction, only one of them will be used; you should
  assume which one is used is indeterminate. (This may be made an error, or at least a warning, in the future.)

  Until upstream issue https://github.com/camsaul/methodical/issues/97 is resolved, you will have to specify which
  method should be applied first in cases of ambiguity using [[methodical.core/prefer-method!]]:

  ```clj
  (m/prefer-method! transformed/transforms ::user-with-location ::user-with-password)
  ```

  If you want to override transforms completely for a model, and ignore transforms from ancestors of a model, you can
  create an `:around` method:

  ```clj
  (defmethod toucan2.tools.transformed/transforms :around ::my-model
    [_model]
    {:field {:in name, :out keyword}})
  ```"
  {:style/indent 1}
  [model transforms]
  `(let [model# ~model]
     (u/maybe-derive model# ::transformed/transformed)
     (m/defmethod transformed/transforms model#
       [~'&model]
       ~transforms)))
