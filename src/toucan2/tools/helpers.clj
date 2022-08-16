(ns toucan2.tools.helpers
  (:require
   [methodical.core :as m]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.tools.transformed :as transformed]
   [toucan2.util :as u]
   [clojure.walk :as walk]
   [toucan2.instance :as instance]
   [toucan2.update :as update]
   [toucan2.model :as model]
   [toucan2.insert :as insert]))

(defn maybe-derive
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))

;;;; [[define-before-select]], [[define-after-select-reducible]], [[define-after-select-each]]

(defn do-before-select [model thunk]
  (u/with-debug-result (format "%s %s" `define-before-select (u/pretty-print model))
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
                   (u/with-debug-result (format "%s %s %s" `define-after-select-each (u/pretty-print model) (u/pretty-print instance))
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
  (update args :query (fn [query]
                        (merge {:select (default-fields model)}
                               query))))

(defmacro define-default-fields [model & body]
  `(let [model# ~model]
     (maybe-derive model# ::default-fields)
     (m/defmethod default-fields model# [~'&model] ~@body)))

;;;; [[define-before-update]], [[define-after-update]]

;; (m/defmulti before-update-transform-changes*
;;   {:arglists '([instance options])}
;;   u/dispatch-on-first-arg)

;; ;; default method is a no-op
;; (m/defmethod before-update-transform-changes* :default
;;   [_ instance _]
;;   instance)

;; (defn update-query->select-query [model update-query-args]
;;   (u/with-debug-result (format "Concert update query args to select query args for %s" (u/pretty-print update-query-args))
;;     (query/build ::select/select model update-query-args)))

;; (defn reducible-instances-matching-update-query
;;   "Return instances of `model` that match conditions from a compiled `update-query` as passed to `update!*`."
;;   [model update-query-args]
;;   (let [select-query-args (update-query->select-query model update-query-args)]
;;     (u/with-debug-result (format "Finding matching instances with query %s" select-query-args)
;;       (select/select-reducible* model select-query-args))))

;; (defn group-by-xform
;;   "Transducer that groups together values into a map of `(key-fn x)` -> `[(val-fn x) ...]`, then reduces that map.

;;     (into {} (group-by-xform even? inc) [1 2 3 4])
;;     ;; ->
;;     {false [2 4], true [3 5]}"
;;   [key-fn val-fn]
;;   (fn [rf]
;;     (let [k->v (atom {})]
;;       (fn
;;         ([] (rf))
;;         ([acc]
;;          ;; optimization: remove the matching-primary-keys if there's just one set of changes to apply.
;;          (let [k->v @k->v
;;                k->v (if (= (bounded-count 2 k->v) 1)
;;                       {(ffirst k->v) nil}
;;                       k->v)]
;;            (reduce
;;             rf
;;             acc
;;             k->v)))
;;         ([acc x]
;;          (let [k (key-fn x)
;;                v (val-fn x)]
;;            (swap! k->v update k #(conj (vec %) v))
;;            acc))))))

;; #_(def ^:dynamic *update-batch-size*
;;   "Maximum number of rows to fetch and update at a time when using `define-before-update` and
;;   `::before-update-transform-matching-rows`."
;;     100)

;; (m/defmethod update/update!* :around [::before-update-transform-matching-rows clojure.lang.IPersistentMap]
;;   [model {updates :set, :as query}]
;;   (u/with-debug-result (format "Doing before update for %s" (u/dispatch-value model))
;;     (if (empty? updates)
;;       (do (u/println-debug (format "Query %s has no changes; skipping rest of update" query))
;;           0)
;;       (letfn [(f [instance]
;;                 (before-update-transform-changes* instance options))]
;;         (try
;;           ;; TODO -- this whole thing should be done in a transaction.
;;           (transduce
;;            (comp
;;             ;; merge in changes for each instance
;;             (map (fn [instance]
;;                    (u/println-debug (format "Found matching instance %s" instance))
;;                    (merge instance updates)))
;;             ;; filter out instances that don't have any changes before calling `f`
;;             (filter (fn [instance]
;;                       (if (seq (instance/changes instance))
;;                         instance
;;                         (u/println-debug (format "Skipping row with PK %s, it has no changes" (model/primary-key-values instance))))))
;;             ;; apply the changes-xform to each instance
;;             (map (fn [instance]
;;                    (u/with-debug-result (format "Apply f to %s" instance)
;;                      (let [result (f instance)]
;;                        (assert (instance/instance? result)
;;                                (format "before-update method for %s should return an instance, got ^%s %s"
;;                                        (u/dispatch-value model)
;;                                        (some-> result class .getCanonicalName)
;;                                        (pr-str result)))
;;                        result))))
;;             ;; filter out the ones that don't have any changes AFTER calling `f`.
;;             (filter (fn [instance]
;;                       (if (seq (instance/changes instance))
;;                         instance
;;                         (u/println-debug (format "Skipping row with PK %s, it has no changes after applying f"
;;                                                  (model/primary-keys instance))))))
;;             ;; TODO -- consider whether we should batch the stuff below. e.g. if we end up matching 1 million objects,
;;             ;; it might not be ideal to keep a million PK value vectors in memory at once. Also, a query with `UPDATE
;;             ;; table WHERE id IN (...)` with a million ids probably isn't going to work so well.
;;             #_(partition-all *update-batch-size*)
;;             ;; Group all the PKs by their changes.
;;             ;;
;;             ;; TODO -- if we had a batched-update method, we wouldn't need this complicated transducer.
;;             (group-by-xform instance/changes (let [pk-keys (model/primary-keys-vec model)]
;;                                                #(mapv % pk-keys)))
;;             ;; do an update call for each distinct set of changes
;;             (map (fn [[changes matching-primary-keys]]
;;                    (let [new-query (-> query
;;                                        (assoc )
;;                                        (build-query/with-changes* changes options)
;;                                        ;; TODO -- :toucan2/with-pks is currently only implemented for HoneySQL, don't
;;                                        ;; assume it works because it might not.
;;                                        (build-query/merge-kv-conditions* {:toucan2/with-pks matching-primary-keys} options))]
;;                      (u/with-debug-result (format "Performing updates with query %s" new-query)
;;                        (next-method model new-query options))))))
;;            (completing (fnil + 0 0))
;;            0
;;            (reducible-instances-matching-update-query model query options))
;;           (catch Throwable e
;;             (throw (ex-info (format "Error in after-update for %s: %s" (u/dispatch-value model) (ex-message e))
;;                             {:model model, :query query}
;;                             e))))))))

;; ;;; TODO -- is this *REALLY* necessary?
;; (defn disallow-next-method-calls [body]
;;   (walk/postwalk
;;    (fn [form]
;;      (if (and (sequential? form)
;;               (= (first form) 'next-method))
;;        (throw (ex-info "Don't call next-method here. It's already called automatically!"
;;                        {:body body, :form form}))
;;        form))
;;    body))

;; (defmacro define-before-update
;;   {:style/indent :defn}
;;   [model [instance-binding] & body]
;;   `(let [model# ~model]
;;      (maybe-derive model# ::before-update-transform-matching-rows)
;;      (m/defmethod before-update-transform-changes* model#
;;        [~'&~instance-binding ~'&options]
;;        (let [result# ~(disallow-next-method-calls `(do ~@body))]
;;          (~'next-method ~'&result# ~'&options)))))

;; (m/defmulti after-update*
;;   {:arglists '([model instance])}
;;   u/dispatch-on-first-arg)

;; (m/defmethod update/update!* :around [::after-update :default]
;;   [model args]
;;   (let [result (next-method model args)]
;;     (u/with-debug-result (format "Doing after-update for %s" (u/dispatch-value model))
;;       (when (pos? result)
;;         (try
;;           (let [rows (reducible-instances-matching-update-query model args)]
;;             (reduce
;;              (fn [_ instance]
;;                (after-update* model instance))
;;              nil
;;              rows))
;;           (catch Throwable e
;;             (throw (ex-info (format "Error in after-update for %s: %s" (u/dispatch-value model) (ex-message e))
;;                             {:model model, :args args}
;;                             e))))))
;;     result))

;; (defmacro define-after-update {:style/indent :defn} [dispatch-value [result-binding] & body]
;;   (let [[model] (dispatch-value-2 dispatch-value)]
;;     `(do
;;        (maybe-derive ~model ::after-update)
;;        (m/defmethod after-update* [~~model]
;;          [~'&~'&model ~result-binding ~'&options]
;;          ~@body))))

;;;; [[define-before-insert]], [[define-after-insert]]

(m/defmulti before-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod insert/insert!* :before ::before-insert
  [model rows]
  (mapv
   (fn [row]
     (try
       (u/with-debug-result ["Do before-insert for %s" model]
         (before-insert model row))
       (catch Throwable e
         (throw (ex-info (format "Error in before-insert for %s: %s" (pr-str model) (ex-message e))
                         {:model model, :row row}
                         e)))))
   rows))

(defmacro define-before-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (derive model# ::before-insert)
     (m/defmethod before-insert model#
       [~'&model ~instance-binding]
       ~@body)))

(m/defmulti after-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(m/defmethod after-insert :around :default
  [model row]
  (u/with-debug-result [(list `after-insert model row)]
    (try
      (next-method model row)
      (catch Throwable e
        (throw (ex-info (format "Error in %s for %s: %s" `after-insert (pr-str model) (ex-message e))
                        {:model model, :row row}
                        e))))))

(def ^:dynamic *return-instances?* true)

(m/defmethod insert/insert!* ::after-insert
  [model rows]
  (if-not *return-instances?*
    (next-method model rows)
    (binding [*return-instances?* false]
      (let [rows  (insert/insert-returning-instances! model rows)
            rows' (mapv
                   (partial after-insert model)
                   rows)]
        (cond-> rows'
          (= insert/*result-type* :row-count)
          count)))))

(defmacro define-after-insert {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (maybe-derive model# ::after-insert)
     (m/defmethod after-insert model#
       [~'&model ~instance-binding]
       ~@body)))

;;;; [[define-before-delete]], [[define-after-delete]]

;; (defn do-before-delete [model delete-query options f]
;;   (helper "before-delete" model
;;           (let [query (-> (build-query/buildable-query* model {} :select options)
;;                           (build-query/with-table* model options)
;;                           (build-query/with-conditions* (build-query/conditions* delete-query) options))]
;;             (u/with-debug-result (format "Fetching matching rows with query %s" query)
;;               (reduce
;;                (fn [_ instance]
;;                  (f instance))
;;                nil
;;                (select/select-reducible [model] query)))))
;;   delete-query)

;; (defmacro define-before-delete {:style/indent :defn} [dispatch-value [instance-binding] & body]
;;   `(m/defmethod mutative/delete!* :before ~(dispatch-value-3 dispatch-value)
;;      [~'&~'&model ~'&query ~'&options]
;;      (do-before-delete ~'&~'&model ~'&query ~'&options
;;                        (fn [~instance-binding]
;;                          ~@body))))

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
     (maybe-derive model# ::transformed/transformed)
     (m/defmethod transformed/transforms* model#
       [~'&model]
       ~transforms)))
