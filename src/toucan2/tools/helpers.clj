(ns toucan2.tools.helpers
  (:require
   [methodical.core :as m]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.tools.transformed :as transformed]
   [toucan2.util :as u]))

(defn maybe-derive
  [child parent]
  (when-not (isa? child parent)
    (derive child parent)))

;;;; [[define-before-select]], [[define-after-select-reducible]], [[define-after-select-each]]

(defn do-before-select [model thunk]
  (u/with-debug-result (format "%s %s" `define-before-select (pr-str model))
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
                   (u/with-debug-result (format "%s %s %s" `define-after-select-each (pr-str model) (pr-str instance))
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

;; (m/defmulti before-update-transform-changes*
;;   {:arglists '([connectableᵈ instanceᵈᵗ options])}
;;   u/dispatch-on-first-two-args
;;   :combo (m.combo.threaded/threading-method-combination :second))

;; ;; default method is a no-op
;; (m/defmethod before-update-transform-changes* :default
;;   [_ instance _]
;;   instance)

;; (defn update-query->select-query [connectable model update-query options]
;;   (let [[connectable options] (conn.current/ensure-connectable connectable model options)
;;         table                 (build-query/table* update-query)
;;         conditions            (build-query/conditions* update-query)]
;;     (-> (build-query/buildable-query* connectable model {} :select options)
;;         (build-query/with-table* table options)
;;         (build-query/with-conditions* conditions options))))

;; (defn reducible-instances-matching-update-query
;;   "Return instances of `model` that match conditions from a compiled `update-query` as passed to `update!*`."
;;   [connectable model update-query options]
;;   (let [[connectable options] (conn.current/ensure-connectable connectable model options)
;;         select-query          (update-query->select-query connectable model update-query options)]
;;     (log/with-trace-no-result ["Finding matching instances with query %s" select-query]
;;       (select/select-reducible [connectable model] select-query (or options {})))))

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

;; (m/defmethod mutative/update!* :around [:default ::before-update-transform-matching-rows :toucan2/buildable-query]
;;   [connectable model {updates :set, :as query} options]
;;   (log/with-debug-no-result ["Doing before update for %s" (u/dispatch-value model)]
;;     (if (empty? updates)
;;       (do (log/tracef "Query %s has no changes; skipping rest of update" query)
;;           0)
;;       (letfn [(f [instance]
;;                 (before-update-transform-changes* connectable instance options))]
;;         (try
;;           ;; TODO -- this whole thing should be done in a transaction.
;;           (transduce
;;            (comp
;;             ;; merge in changes for each instance
;;             (map (fn [instance]
;;                    (log/tracef "Found matching instance %s" instance)
;;                    (merge instance updates)))
;;             ;; filter out instances that don't have any changes before calling `f`
;;             (filter (fn [instance]
;;                       (if (seq (instance/changes instance))
;;                         instance
;;                         (log/tracef "Skipping row with PK %s, it has no changes" (model/primary-key-values instance)))))
;;             ;; apply the changes-xform to each instance
;;             (map (fn [instance]
;;                    (log/with-trace ["Apply f to %s" instance]
;;                      (let [result (f instance)]
;;                        (assert (instance/toucan2-instance? result)
;;                                (format "before-update method for %s should return an instance, got ^%s %s"
;;                                        (u/dispatch-value model)
;;                                        (some-> result class .getCanonicalName)
;;                                        (pr-str result)))
;;                        result))))
;;             ;; filter out the ones that don't have any changes AFTER calling `f`.
;;             (filter (fn [instance]
;;                       (if (seq (instance/changes instance))
;;                         instance
;;                         (log/tracef "Skipping row with PK %s, it has no changes after applying f"
;;                                     (model/primary-key-keys connectable instance)))))
;;             ;; TODO -- consider whether we should batch the stuff below. e.g. if we end up matching 1 million objects,
;;             ;; it might not be ideal to keep a million PK value vectors in memory at once. Also, a query with `UPDATE
;;             ;; table WHERE id IN (...)` with a million ids probably isn't going to work so well.
;;             #_(partition-all *update-batch-size*)
;;             ;; Group all the PKs by their changes.
;;             ;;
;;             ;; TODO -- if we had a batched-update method, we wouldn't need this complicated transducer.
;;             (group-by-xform instance/changes (let [pk-keys (vec (model/primary-key-keys connectable model))]
;;                                                #(mapv % pk-keys)))
;;             ;; do an update call for each distinct set of changes
;;             (map (fn [[changes matching-primary-keys]]
;;                    (let [new-query (-> query
;;                                        (build-query/with-changes* changes options)
;;                                        ;; TODO -- :toucan2/with-pks is currently only implemented for HoneySQL, don't
;;                                        ;; assume it works because it might not.
;;                                        (build-query/merge-kv-conditions* {:toucan2/with-pks matching-primary-keys} options))]
;;                      (log/with-trace ["Performing updates with query %s" new-query]
;;                        (next-method connectable model new-query options))))))
;;            (completing (fnil + 0 0))
;;            0
;;            (reducible-instances-matching-update-query connectable model query options))
;;           (catch Throwable e
;;             (throw (ex-info (format "Error in after-update for %s: %s" (u/dispatch-value model) (ex-message e))
;;                             {:model model, :query query}
;;                             e))))))))

;; (defn disallow-next-method-calls [body]
;;   (walk/postwalk
;;    (fn [form]
;;      (if (and (sequential? form)
;;               (= (first form) 'next-method))
;;        (throw (ex-info "Don't call next-method here. It's already called automatically!"
;;                        {:body body, :form form}))
;;        form))
;;    body))

;; (defmacro define-before-update {:style/indent :defn} [dispatch-value [instance-binding] & body]
;;   (let [[connectable-dv model-dv] (dispatch-value-2 dispatch-value)]
;;     `(do
;;        (u/maybe-derive ~model-dv ::before-update-transform-matching-rows)
;;        (m/defmethod before-update-transform-changes* [~connectable-dv ~model-dv]
;;          [~'&connectable ~instance-binding ~'&options]
;;          (let [result# ~(disallow-next-method-calls `(do ~@body))]
;;            (~'next-method ~'&connectable result# ~'&options))))))

;; (m/defmulti after-update*
;;   {:arglists '([connectableᵈ modelᵈ instanceᵗ options])}
;;   u/dispatch-on-first-two-args
;;   :combo (m.combo.threaded/threading-method-combination :third))

;; (m/defmethod mutative/update!* :around [:default ::after-update :default]
;;   [connectable model update-query options]
;;   (let [result (next-method connectable model update-query options)]
;;     (log/with-debug-no-result ["Doing after-update for %s" (u/dispatch-value model)]
;;       (when (pos? result)
;;         (try
;;           (let [rows (reducible-instances-matching-update-query connectable model update-query options)]
;;             (reduce
;;              (fn [_ instance]
;;                (after-update* connectable model instance options))
;;              nil
;;              rows))
;;           (catch Throwable e
;;             (throw (ex-info (format "Error in after-update for %s: %s" (u/dispatch-value model) (ex-message e))
;;                             {:model model, :update-query update-query, :options options}
;;                             e))))))
;;     result))

;; (defmacro define-after-update {:style/indent :defn} [dispatch-value [result-binding] & body]
;;   (let [[connectable model] (dispatch-value-2 dispatch-value)]
;;     `(do
;;        (u/maybe-derive ~model ::after-update)
;;        (m/defmethod after-update* [~connectable ~model]
;;          [~'&connectable ~'&model ~result-binding ~'&options]
;;          ~@body))))

;; (defn do-before-insert [connectable model query options f]
;;   (helper "before-insert" model
;;     (update query :values (fn [rows]
;;                             (mapv
;;                              (fn [row]
;;                                (f (instance/instance connectable model row)))
;;                              rows)))))

;; (defmacro define-before-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
;;   `(m/defmethod mutative/insert!* :before ~(dispatch-value-3 dispatch-value)
;;      [~'&connectable ~'&model ~'&query ~'&options]
;;      (do-before-insert ~'&connectable ~'&model ~'&query ~'&options
;;                        (fn [~instance-binding]
;;                          ~@body))))

;; (m/defmulti after-insert*
;;   {:arglists '([connectableᵈ instanceᵈᵗ options])}
;;   u/dispatch-on-first-two-args
;;   :combo (m.combo.threaded/threading-method-combination :second))

;; (m/defmethod mutative/insert!* [:default ::after-insert :default]
;;   [connectable model query options]
;;   (try
;;     (let [return-keys?        (get-in options [:next.jdbc :return-keys])
;;           options             (-> options
;;                                   ;; TODO -- this is `next.jdbc`-specific -- need a general way to specify
;;                                   ;; `:return-keys` behavior.
;;                                   (assoc :reducible? true)
;;                                   (assoc-in [:next.jdbc :return-keys] true))
;;           reducible-query     (next-method connectable model query options)
;;           pks                 (into [] (map (select/select-pks-fn connectable model)) reducible-query)
;;           reducible-instances (select/select-reducible
;;                                [connectable model]
;;                                :toucan2/with-pks pks
;;                                {}
;;                                (update options :next.jdbc dissoc :return-keys))]
;;       (transduce
;;        (map realize/realize)
;;        (completing
;;         (fn [acc instance]
;;           (after-insert* connectable instance options)
;;           (if return-keys?
;;             (conj acc (model/primary-key-values instance))
;;             (inc acc))))
;;        (if return-keys? [] 0)
;;        reducible-instances))
;;     (catch Throwable e
;;       (throw (ex-info (format "Error in after insert for %s: %s" (u/dispatch-value model) (ex-message e))
;;                       {:model model, :query query, :options options}
;;                       e)))))

;; (defmacro define-after-insert {:style/indent :defn} [dispatch-value [instance-binding] & body]
;;   (let [[connectable model] (dispatch-value-2 dispatch-value)]
;;     `(do
;;        (u/maybe-derive ~model ::after-insert)
;;        (m/defmethod after-insert* [~connectable ~model]
;;          [~'&connectable ~instance-binding ~'&options]
;;          ~@body))))

;; (defn do-before-delete [connectable model delete-query options f]
;;   (helper "before-delete" model
;;     (let [query (-> (build-query/buildable-query* connectable model {} :select options)
;;                     (build-query/with-table* model options)
;;                     (build-query/with-conditions* (build-query/conditions* delete-query) options))]
;;       (log/with-trace ["Fetching matching rows with query %s" query]
;;         (reduce
;;          (fn [_ instance]
;;            (f instance))
;;          nil
;;          (select/select-reducible [connectable model] query)))))
;;   delete-query)

;; (defmacro define-before-delete {:style/indent :defn} [dispatch-value [instance-binding] & body]
;;   `(m/defmethod mutative/delete!* :before ~(dispatch-value-3 dispatch-value)
;;      [~'&connectable ~'&model ~'&query ~'&options]
;;      (do-before-delete ~'&connectable ~'&model ~'&query ~'&options
;;                        (fn [~instance-binding]
;;                          ~@body))))

;; ;; TODO
;; #_(defmacro define-after-delete {:style/indent :defn} [dispatch-value [a-binding] & body]
;;     (m/defmethod mutative/delete!* :after ~(dispatch-value-3 dispatch-value)
;;       [~'&connectable ~'&model _ ~'&options]
;;       ~@body))

;; (defmacro define-keys-for-automagic-hydration
;;   {:style/indent 1}
;;   [dispatch-value & ks]
;;   (let [[connectable model] (dispatch-value-2 dispatch-value)]
;;     `(do
;;        ~@(for [k ks]
;;            `(m/defmethod hydrate/table-for-automagic-hydration* [~connectable ~model ~k]
;;               [~'_ ~'_ ~'_]
;;               ~model)))))

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
