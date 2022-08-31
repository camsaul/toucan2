(ns toucan2.tools.after
  "Common code shared by various `after-` methods. Since the `after` methods operate over instances, we need to upgrade
  `result-type/pks` and `result-type/update-count` queries to `result-type/instances`, run them with the 'upgraded'
  result type, run our after stuff on each row, and then return the original results."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(m/defmulti each-row-fn
  "Should return a function with the signature

  ```clj
  (f instance)
  ```

  This function is only done for side-effects for query types that return update counts or PKs."
  {:arglists '([query-type₁ model₂])}
  u/dispatch-on-first-two-args)

(m/defmethod each-row-fn :after :default
  [query-type f]
  (assert (fn? f)
          (format "Expected each-row-fn for query type %s to return a function, got ^%s %s"
                  (u/safe-pr-str query-type)
                  (some-> f class .getCanonicalName)
                  (u/safe-pr-str f)))
  f)

(m/defmulti ^:no-doc result-type-rf
  {:arglists '([original-query-type₁ model rf])}
  u/dispatch-on-first-arg)

(m/defmethod result-type-rf :toucan.result-type/update-count
  [_original-query-type _model rf]
  ((map (constantly 1))
   rf))

(m/defmethod result-type-rf :toucan.result-type/pks
  [_original-query-type model rf]
  (let [pks-fn (model/select-pks-fn model)]
    ((map (fn [row]
            (assert (map? row))
            (pks-fn row)))
     rf)))

(m/defmethod result-type-rf :toucan.result-type/instances
  [original-query-type model rf]
  (let [row-fn (each-row-fn original-query-type model)
        row-fn (fn [row]
                 (u/try-with-error-context ["Apply after row fn" {::query-type original-query-type, ::model model}]
                   (u/with-debug-result ["Apply after %s for %s" original-query-type model]
                     (let [result (row-fn row)]
                       ;; if the row fn didn't return something (not generally necessary for something like
                       ;; `after-update` which is always done for side effects) then return the original row. We still
                       ;; need it for stuff like getting the PKs back out.
                       (if (some? result)
                         result
                         row)))))]
    ((map row-fn) rf)))

(m/defmethod pipeline/transduce-execute [#_query-type     ::query-type
                                                #_model          ::model
                                                #_compiled-query :default]
  [rf query-type model sql-args]
  (let [upgraded-type (pipeline/similar-query-type-returning query-type :toucan.result-type/instances)
        _             (assert upgraded-type (format "Don't know how to upgrade a %s query to one returning instances"
                                                    query-type))
        rf*           (result-type-rf query-type model rf)
        m             (if (= query-type upgraded-type)
                        next-method
                        pipeline/transduce-execute)]
    (m rf* upgraded-type model sql-args)))

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
