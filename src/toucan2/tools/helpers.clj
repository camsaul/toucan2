(ns toucan2.tools.helpers
  (:require
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.tools.transformed :as transformed]
   [toucan2.util :as u]))

;;;; [[define-before-select]], [[define-after-select-reducible]], [[define-after-select-each]]

(defn do-before-select [model thunk]
  (u/with-debug-result ["%s %s" `define-before-select model]
    (try
      (thunk)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `define-before-select (pr-str model) (ex-message e))
                        {:model model}
                        e))))))

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

(defmacro define-after-select-reducible
  {:style/indent :defn}
  [model [reducible-query-binding] & body]
  `(m/defmethod select/select-reducible* :after ~model
     [~'&model ~reducible-query-binding]
     ~@body))

(defn do-after-select-each [model reducible-query f]
  (eduction (map (fn [instance]
                   (u/with-debug-result [(list `define-after-select-each model instance)]
                     (try
                       (f instance)
                       (catch Throwable e
                         (throw (ex-info (format "Error in %s for %s: %s"
                                                 `define-after-select-each (pr-str model) (ex-message e))
                                         {:model model, :instance instance}
                                         e)))))))
            reducible-query))

(defmacro define-after-select-each
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(define-after-select-reducible ~model [reducible-query#]
     (do-after-select-each ~'&model reducible-query# (fn [~instance-binding] ~@body))))

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

(m/defmethod delete/delete!* :around ::before-delete
  [model parsed-args]
  (conn/with-transaction [_conn (model/deferred-current-connectable model)]
    (transduce
     (map (fn [row]
            (before-delete model row)))
     (constantly nil)
     nil
     (select/select-reducible* model parsed-args))
    (next-method model parsed-args)))

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

;;;; [[define-keys-for-automagic-hydration]]

#_(defmacro define-keys-for-automagic-hydration
  {:style/indent 1}
  [dispatch-value & ks]
  (let [[model] (dispatch-value-2 dispatch-value)]
    `(do
       ~@(for [k ks]
           `(m/defmethod hydrate/table-for-automagic-hydration* [~~model ~k]
              [~'_ ~'_ ~'_]
              ~model)))))

;;;; [[deftransforms]]

;;; TODO -- move this to [[toucan2.tools.transformed]]
;;;
;;; Not sure we even need this. Maybe remove this and just let people implement [[transformed/transforms]] directly.
(defmacro deftransforms
  "Define type transforms to use for a specific model. `transforms` should be a map of

    {:column-name {:in <fn>, :out <fn>}}

  `:in` transforms are applied to values going over the wire to the database; these generally only applied to values
  passed at or near the top level to various functions; don't expect Toucan 2 to parse your SQL to find out which
  parameter corresponds to what in order to apply transforms or to apply transforms inside JOINS in hand-written
  HoneySQL. That said, unless you're doing something weird your transforms should generally get applied.

  `:out` transforms are applied to values coming out of the database; since nothing weird really happens there this is
  done consistently.

  Transform functions for either case are skipped for `nil` values.

  Example:

    (deftransforms :models/user
      {:type {:in name, :out keyword}})

  You can also define transforms independently, and derive a model from them:

    (deftransforms ::type-keyword
      {:type {:in name, :out keyword}})

    (derive :models/user ::type-keyword)
    (derive :models/user ::some-other-transform)

  Don't derive a model from multiple `deftransforms` for the same key in the same direction.

  When multiple transforms match a given model they are combined into a single map of transforms with `merge-with
  merge`. If multiple transforms match a given column in a given direction, only one of them will be used; you should
  assume which one is used is indeterminate. (This may be made an error, or at least a warning, in the future.)

  Until upstream issue https://github.com/camsaul/methodical/issues/97 is resolved, you will have to specify which
  method should be applied first in cases of ambiguity using [[methodical.core/prefer-method!]]:

    (m/prefer-method! transformed/transforms ::user-with-location ::user-with-password)

  If you want to override transforms completely for a model, and ignore transforms from ancestors of a model, you can
  create an `:around` method:

    (defmethod toucan2.tools.transformed/transforms :around ::my-model
      [_model]
      {:field {:in name, :out keyword}})"
  {:style/indent 1}
  [model transforms]
  `(let [model# ~model]
     (u/maybe-derive model# ::transformed/transformed)
     (m/defmethod transformed/transforms model#
       [~'&model]
       ~transforms)))
