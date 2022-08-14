(ns toucan2.tools.transformed
  (:require
   [methodical.core :as m]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(m/defmulti transforms*
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod transforms* :default
  [_model]
  nil)

;;; I originally considered walking and transforming the HoneySQL, but decided against it because it's too ambiguous.
;;; It's too hard to tell if
;;;
;;;    [:= :id :col]
;;;
;;; means
;;;
;;; A) `:id` is a column identifier key, and `:col` is a value for it that we should transform
;;; B) `:col` is a column identifier key, and `:id` is a value for it that we should transform
;;; C) `:id` is a column identifier key, but `:col` is just a reference to another column, and we shouldn't transform it
;;; D) `:col` is a column identifier key, but `:id` is just a reference to another column, and we shouldn't transform it
;;;
;;; It's also hard to know what are the "values" of every different type of filter clause (including custom ones we
;;; don't know about). I think leaving HoneySQL as an outlet to bypass type transforms makes sense for now. This also
;;; avoids locking us in to HoneySQL too much

(defn- transform-condition-value [xform v]
  (try
    (cond
      (sequential? v)
      (into [(first v)]
            (map (fn xform* [v]
                   (if (sequential? v)
                     (mapv xform* v)
                     (xform v))))
            (rest v))

      ;; only apply xform if the value is non-nil.
      (some? v)
      (xform v)

      :else
      nil)
    (catch Throwable e
      (throw (ex-info (format "Error transforming %s: %s" (pr-str v) (ex-message e))
                      {:v v, :transform xform}
                      e)))))

(defn transform-kv-args [kv-args transforms]
  {:pre [(seq transforms)]}
  (into {} (for [[k v] kv-args]
             [k (if-let [xform (get transforms k)]
                  (transform-condition-value xform v)
                  v)])))

(defn transform-pk [pk-vals model transforms]
  (if-not (sequential? pk-vals)
    (first (transform-pk [pk-vals] model transforms))
    (let [pk-keys (model/primary-keys model)]
      (mapv
       (fn [k v]
         (if-let [xform (get transforms k)]
           (xform v)
           v))
       pk-keys
       pk-vals))))

(defn wrapped-transforms [model direction]
  (when-let [transforms (not-empty (transforms* model))]
    ;; make the transforms map an instance so we can get appropriate magic map behavior when looking for the
    ;; appropriate transform for a given key.
    (instance/instance
     model
     (into {} (for [[k direction->xform] transforms
                    :let                 [xform (get direction->xform direction)]
                    :when                xform]
                [k (fn [v]
                     (try
                       (xform v)
                       (catch Throwable e
                         (throw (ex-info (format "Error transforming %s %s value %s: %s"
                                                 (pr-str model)
                                                 (pr-str k)
                                                 (pr-str v)
                                                 (ex-message e))
                                         {:model model, :k k, :v v, :xform xform}
                                         e)))))])))))

(defn in-transforms [model]
  (wrapped-transforms model :in))

(defn apply-in-transforms
  [model {:keys [kv-args], :as args}]
  (if-let [transforms (not-empty (when (seq kv-args)
                                   (in-transforms model)))]
    (u/with-debug-result (format "Apply transforms to kv-args %s" (pr-str kv-args))
      (u/println-debug (pr-str transforms))
      (cond-> args
        (seq kv-args)                (update :kv-args transform-kv-args transforms)
        (some? (:toucan/pk kv-args)) (update-in [:kv-args :toucan/pk] transform-pk model transforms)))
    args))

(m/defmethod query/parse-args :after [::select/select ::transformed]
  [_query-type model args]
  (apply-in-transforms model args))

(defn apply-row-transform [instance k xform]
  ;; The "Special Optimizations" below *should* be the default case, but if some other aux methods are in place or
  ;; custom impls it might not be; things should still work normally either way.
  ;;
  ;; Special Optimization 1: if `instance` is an `IInstance`, and original and current are the same object, this only
  ;; applies `xform` once.
  (instance/update-original-and-current
   instance
   (fn [row]
     ;; Special Optimization 2: if the underlying original/current maps of `instance` are instances of `IRow` (which
     ;; themselves have underlying key->value thunks) we can compose the thunk itself rather than immediately
     ;; realizing and transforming the value. This means transforms don't get applied to values that are never
     ;; realized.
     ;;
     ;; TODO FIXME -- need to copy over the magic result-row code from the old Toucan 2 codebase or figure out a
     ;; different way to get this magical optimization.
     #_(if-let [thunks (result-row/thunks row)]
       (result-row/with-thunks row (update thunks k (fn [thunk]
                                                        (comp xform thunk))))
       (update row k xform))
     (update row k xform))))

(defn out-transforms [model]
  (wrapped-transforms model :out))

(defn row-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (let [transform-fns (for [[k xform] transforms]
                        (fn [instance]
                          (if (contains? instance k)
                            (apply-row-transform instance k xform)
                            instance)))]
    (apply comp transform-fns)))

(defn transform-results [model reducible-query]
  (if-let [transforms (not-empty (out-transforms model))]
    (u/with-debug-result (format "Apply transforms %s to results" model)
      (u/println-debug (pr-str transforms))
      (eduction
       (map (row-transform-fn transforms))
       reducible-query))
    reducible-query))

(m/defmethod select/select-reducible* :around [::transformed :default]
  [model parsed-args]
  (let [reducible-query (next-method model parsed-args)]
    (transform-results model reducible-query)))

(m/defmethod query/build :before [::update/update ::transformed :default]
  [_query-type model {:keys [query], :as args}]
  (if-let [transforms (not-empty (in-transforms model))]
    (u/with-debug-result (format "Apply %s transforms to %s" transforms args)
      (let [args (-> args
                     (update :kv-args merge query)
                     (dissoc :query))]
        (-> (apply-in-transforms model args)
            (update :changes transform-kv-args transforms))))
    args))

(defn transform-insert-rows [[first-row :as rows] transforms]
  ;; all rows should have the same keys, so we just need to look at the keys in the first row
  (let [row-xforms (for [k (keys first-row)
                         :let [xform (get transforms k)]
                         :when xform]
                     (fn [row]
                       (update row k (fn [v]
                                       (if (some? v)
                                         (xform v)
                                         v)))))
        row-xform (apply comp row-xforms)]
    (map row-xform rows)))

(m/defmethod query/parse-args :after [::insert/insert ::transformed]
  [_query-type model rows]
  (if-let [transforms (in-transforms model)]
    (u/with-debug-result (format "Apply %s transforms to %s" transforms rows)
      (transform-insert-rows rows transforms))
    rows))

(m/defmethod insert/insert!* :after ::transformed :default
  [model results]
  (if (integer? results)
    results
    (transform-results model results)))
