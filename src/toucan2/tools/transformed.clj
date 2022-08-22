(ns toucan2.tools.transformed
  (:require
   [methodical.core :as m]
   [methodical.impl.combo.operator :as m.combo.operator]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.update :as update]
   [toucan2.util :as u]))

;;; HACK Once https://github.com/camsaul/methodical/issues/96 is fixed we can remove this.
(alter-meta! #'m.combo.operator/combine-methods-with-operator assoc :private false)

;; combine the results of all matching methods into one map.
(m.combo.operator/defoperator ::merge-transforms
  [methods invoke]
  (transduce
   (map invoke)
   ;; for the time being keep the values from map returned by the more-specific method in preference to the ones
   ;; returned by the less-specific methods.
   (completing (fn [m1 m2]
                 ;; TODO -- we should probably throw an error if one of the transforms is stomping on the other.
                 (merge-with merge m2 m1)))
   {}
   methods))

(defonce ^{:doc "Return a map of

    {column-name {:in <fn>, :out <fn>}}

  For a given `model`, all matching transforms are combined with `merge-with merge` in an indeterminate order, so don't
  try to specify multiple transforms for the same column in the same direction for a given model -- compose your
  transform functions instead if you want to do that. See [[toucan2.tools.helpers/deftransforms]] for more info."
           :arglists '([model])}
  transforms
  ;; TODO -- this has to be uncached for now because of https://github.com/camsaul/methodical/issues/98
  (m/uncached-multifn
   (m/standard-multifn-impl
    (m.combo.operator/operator-method-combination ::merge-transforms)
    ;; TODO -- once https://github.com/camsaul/methodical/issues/97 is implemented, use that.
    (m/standard-dispatcher u/dispatch-on-first-arg)
    (m/standard-method-table))
   {:ns *ns*, :name `transforms}))

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
  (try
    (when-let [transforms (not-empty (transforms model))]
      ;; make the transforms map an instance so we can get appropriate magic map behavior when looking for the
      ;; appropriate transform for a given key.
      (instance/instance
       model
       (into {} (for [[k direction->xform] transforms
                      :let                 [xform (get direction->xform direction)]
                      :when                xform]
                  [k (fn xform-fn [v]
                       (try
                         (xform v)
                         (catch Throwable e
                           (throw (ex-info (format "Error transforming %s %s value %s: %s"
                                                   (pr-str model)
                                                   (pr-str k)
                                                   (pr-str v)
                                                   (ex-message e))
                                           {:model model, :k k, :v v, :xform xform}
                                           e)))))]))))
    (catch Throwable e
      (throw (ex-info (format "Error calculating %s transforms for %s: %s" direction (pr-str model) (ex-message e))
                      {:model model, :direction direction}
                      e)))))

(defn in-transforms [model]
  (wrapped-transforms model :in))

(defn apply-in-transforms
  [model {:keys [kv-args], :as args}]
  (if-let [transforms (not-empty (when (seq kv-args)
                                   (in-transforms model)))]
    (u/with-debug-result ["Apply transforms to kv-args %s" kv-args]
      (u/println-debug [transforms])
      (cond-> args
        (seq kv-args)                (update :kv-args transform-kv-args transforms)
        (some? (:toucan/pk kv-args)) (update-in [:kv-args :toucan/pk] transform-pk model transforms)))
    args))

(m/defmethod query/parse-args :after [::select/select ::transformed]
  [_query-type model args]
  (apply-in-transforms model args))

(m/defmethod query/parse-args :after [::delete/delete ::transformed]
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
     (u/with-debug-result ["Transform %s %s" k (get row k)]
       (try
         (update row k xform)
         (catch Throwable e
           (throw (ex-info (format "Error transforming %s %s: %s" (pr-str k) (pr-str (get row k)) (ex-message e))
                           {:k k, :v (get row k), :row row, :xform xform}
                           e))))))))

(defn out-transforms [model]
  (wrapped-transforms model :out))

(defn- apply-row-transform-fn [k xform]
  (fn [instance]
    (cond-> instance
      (contains? instance k) (apply-row-transform k xform))))

(defn row-transform-fn [transforms]
  {:pre [(seq transforms)]}
  (reduce
   (fn [f [k xform]]
     (comp (apply-row-transform-fn k xform)
      f))
   identity
   transforms))

(defn transform-results [model reducible-query]
  (if-let [transforms (not-empty (out-transforms model))]
    (u/with-debug-result [(list `transform-results model)]
      (u/println-debug [transforms])
      reducible-query
      (eduction
       (map (row-transform-fn transforms))
       reducible-query))
    reducible-query))

;;; TODO -- shouldn't this be an `:after` method?
(m/defmethod select/select-reducible* :around ::transformed
  [model parsed-args]
  (let [reducible-query (next-method model parsed-args)]
    (transform-results model reducible-query)))

(m/defmethod query/build :before [::update/update ::transformed :default]
  [_query-type model {:keys [query], :as args}]
  (if-let [transforms (not-empty (in-transforms model))]
    (u/with-debug-result ["Apply %s transforms to %s" transforms args]
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

(m/defmethod op/reducible* :before [::insert/insert ::transformed]
  [_query-type model parsed-args]
  (assert (isa? model ::transformed))
  (if-let [transforms (in-transforms model)]
    (u/with-debug-result ["Apply %s transforms to %s" transforms parsed-args]
      (update parsed-args :rows transform-insert-rows transforms))
    parsed-args))

(m/defmethod op/reducible-returning-pks* :after [::insert/insert ::transformed]
  [_query-type model reducible-results]
  (let [pk-keys (model/primary-keys model)]
    (eduction
     (map (if (= (count pk-keys) 1)
            (first pk-keys)
            (juxt pk-keys)))
     (transform-results model (eduction
                               (map (fn [pk-vec]
                                      (zipmap pk-keys (if (sequential? pk-vec)
                                                        pk-vec
                                                        [pk-vec]))))
                               reducible-results)))))
