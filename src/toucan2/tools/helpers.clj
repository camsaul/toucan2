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

(defmacro define-default-fields [model & body]
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

(defmacro deftransforms
  "`transforms` should be a map of

    {column-name {:in <fn>, :out <fn>}}"
  {:style/indent 1}
  [model transforms]
  `(let [model# ~model]
     (u/maybe-derive model# ::transformed/transformed)
     (m/defmethod transformed/transforms* model#
       [~'&model]
       ~transforms)))
