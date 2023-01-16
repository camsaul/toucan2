(ns toucan2.tools.after
  "Common code shared by various `after-` methods. Since the `after` methods operate over instances, we need to upgrade
  `result-type/pks` and `result-type/update-count` queries to `result-type/instances`, run them with the 'upgraded'
  result type, run our after stuff on each row, and then return the original results."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti each-row-fn
  "Should return a function with the signature

  ```clj
  (f instance)
  ```

  This function is only done for side-effects for query types that return update counts or PKs."
  {:arglists '([query-type₁ model₂]), :defmethod-arities #{2}}
  u/dispatch-on-first-two-args)

(m/defmethod each-row-fn :after :default
  [query-type f]
  (assert (fn? f)
          (format "Expected each-row-fn for query type %s to return a function, got ^%s %s"
                  (pr-str query-type)
                  (some-> f class .getCanonicalName)
                  (pr-str f)))
  f)

(m/defmethod pipeline/results-transform [#_query-type :toucan.result-type/instances #_model ::model]
  [query-type model]
  ;; if there's no [[each-row-fn]] for this `query-type` then we don't need to apply any transforms. Example: maybe a
  ;; model has an [[each-row-fn]] for `INSERT` queries, but not for `UPDATE`. Since the model derives from `::model`, we
  ;; end up here either way. But if `query-type` is `UPDATE` we shouldn't touch the query.
  (if (m/is-default-primary-method? each-row-fn [query-type model])
    (next-method query-type model)
    (let [row-fn (each-row-fn query-type model)
          row-fn (fn [row]
                   (u/try-with-error-context ["Apply after row fn" {::query-type query-type, ::model model}]
                     (log/debugf :results "Apply after %s for %s" query-type model)
                     (let [result (row-fn row)]
                       ;; if the row fn didn't return something (not generally necessary for something like
                       ;; `after-update` which is always done for side effects) then return the original row. We still
                       ;; need it for stuff like getting the PKs back out.
                       (if (some? result)
                         result
                         row))))
          xform  (map row-fn)]
      (comp xform
            (next-method query-type model)))))

(m/defmulti ^:private result-type-rf
  {:arglists '([original-query-type₁ model rf]), :defmethod-arities #{3}}
  u/dispatch-on-first-arg)

(m/defmethod result-type-rf :toucan.result-type/update-count
  "Reducing function transform that will return the count of rows."
  [_original-query-type _model rf]
  ((map (constantly 1))
   rf))

(m/defmethod result-type-rf :toucan.result-type/pks
  "Reducing function transform that will return just the PKs (as single values or vectors of values) by getting them from
  row maps (instances)."
  [_original-query-type model rf]
  (let [pks-fn (model/select-pks-fn model)]
    ((map (fn [row]
            (assert (map? row))
            (pks-fn row)))
     rf)))

(derive ::query-type :toucan.query-type/abstract)

(m/defmethod pipeline/transduce-query [#_query-type     ::query-type
                                       #_model          ::model
                                       #_resolved-query :default]
  "'Upgrade' a query so that it returns instances, and run the upgraded query so that we can apply [[each-row-fn]] to the
  results. Then apply [[result-type-rf]] to the results of the original expected type are ultimately returned."
  [rf query-type model parsed-args resolved-query]
  (cond
    ;; only "upgrade" the query if there's an applicable [[each-row-fn]] to apply.
    (m/is-default-primary-method? each-row-fn [query-type model])
    (next-method rf query-type model parsed-args resolved-query)

    ;; there's no need to "upgrade" the query if it's already returning instances.
    (isa? query-type :toucan.result-type/instances)
    (next-method rf query-type model parsed-args resolved-query)

    ;; otherwise we need to run an upgraded query but then transform the results back to the originals
    ;; with [[result-type-rf]]
    :else
    (let [upgraded-type (types/similar-query-type-returning query-type :toucan.result-type/instances)
          _             (assert upgraded-type (format "Don't know how to upgrade a %s query to one returning instances"
                                                      query-type))
          rf*           (result-type-rf query-type model rf)]
      (pipeline/transduce-query rf* upgraded-type model parsed-args resolved-query))))

(defn ^:no-doc ^{:style/indent [:form]} define-after-impl
  [next-method query-type model row-fn]
  (let [f      (fn [row]
                 (or (row-fn row)
                     row))
        next-f (when next-method
                 (next-method query-type model))]
    (if next-f
      (comp next-f f)
      f)))

(defmacro define-after
  [query-type model [instance-binding] & body]
  `(do
     (u/maybe-derive ~model ::model)
     (m/defmethod each-row-fn [~query-type ~model]
       [~'&query-type ~'&model]
       (define-after-impl ~'next-method
                          ~'&query-type
                          ~'&model
                          (fn [~instance-binding]
                            ~@body)))))

(s/fdef define-after*
  :args (s/cat :query-type #(isa? % :toucan.query-type/*)
               :model      some?
               :bindings   (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body       (s/+ any?))
  :ret any?)
