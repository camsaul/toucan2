(ns toucan2.tools.transformed
  (:require
   [clojure.spec.alpha :as s]
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
  [method-fns invoke]
  (transduce
   (map invoke)
   ;; for the time being keep the values from map returned by the more-specific method in preference to the ones
   ;; returned by the less-specific methods.
   (completing (fn [m1 m2]
                 ;; TODO -- we should probably throw an error if one of the transforms is stomping on the other.
                 (merge-with merge m2 m1)))
   {}
   method-fns))

(defonce ^{:doc "Return a map of

  ```clj
  {column-name {:in <fn>, :out <fn>}}
  ```

  For a given `model`, all matching transforms are combined with `merge-with merge` in an indeterminate order, so don't
  try to specify multiple transforms for the same column in the same direction for a given model -- compose your
  transform functions instead if you want to do that. See [[toucan2.tools.transformed/deftransforms]] for more info."
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
    nil))

(defn- transform-kv-args [kv-args k->transform]
  {:pre [(map? k->transform) (seq k->transform)]}
  (into {} (for [[k v] kv-args]
             [k (if-let [xform (get k->transform k)]
                  (transform-condition-value xform v)
                  v)])))

(defn- transform-pk [pk-vals model k->transform]
  (if-not (sequential? pk-vals)
    (first (transform-pk [pk-vals] model k->transform))
    (let [pk-keys (model/primary-keys model)]
      (mapv
       (fn [k v]
         (if-let [xform (get k->transform k)]
           (xform v)
           v))
       pk-keys
       pk-vals))))

(defn- wrapped-transforms
  "Get the [[transforms]] functions for a model in a either the `:in` or `:out` direction; wrap the functions in
  `try-catch` forms so we can meaningful error messages if they fail."
  [model direction]
  (u/try-with-error-context ["calculate transforms" {::model model, ::direction direction}]
    (when-let [k->direction->transform (not-empty (transforms model))]
      ;; make the transforms map an instance so we can get appropriate magic map behavior when looking for the
      ;; appropriate transform for a given key.
      (instance/instance
       model
       (into {} (for [[k direction->xform] k->direction->transform
                      :let                 [xform (get direction->xform direction)]
                      :when                xform]
                  [k (fn xform-fn [v]
                       (u/try-with-error-context ["apply transform" {::transforms k->direction->transform
                                                                     ::model      model
                                                                     ::k          k
                                                                     ::v          v
                                                                     ::xform      xform}]
                         (xform v)))]))))))

(defn- in-transforms [model]
  (wrapped-transforms model :in))

;;; TODO -- this should be private, but it has to be public for now to implement the hack in
;;; [[toucan.models/do-pre-update]]
(defn apply-in-transforms
  "Apply the [[in-transforms]] for a `model` to the `:kv-args` in `args`."
  [model {:keys [kv-args], :as args}]
  {:post [(map? %)]}
  (if-let [k->transform (not-empty (when (seq kv-args)
                                     (in-transforms model)))]
    (u/try-with-error-context ["apply in transforms" {::model model, ::transforms k->transform, ::parsed-args args}]
      (u/with-debug-result ["Apply transforms to kv-args %s" kv-args]
        (u/println-debug [k->transform])
        (cond-> args
          (seq kv-args)                (update :kv-args transform-kv-args k->transform)
          (some? (:toucan/pk kv-args)) (update-in [:kv-args :toucan/pk] transform-pk model k->transform))))
    args))

(m/defmethod query/build :before [::select/select ::transformed :default]
  [query-type model parsed-args]
  (if (isa? query-type ::op/return-instances-from-pks)
    parsed-args
    (apply-in-transforms model parsed-args)))

(m/defmethod query/build :before [::delete/delete ::transformed :default]
  [_query-type model parsed-args]
  (apply-in-transforms model parsed-args))

(defn- apply-row-transform [instance k xform]
  ;; The "Special Optimizations" below *should* be the default case, but if some other aux methods are in place or
  ;; custom impls it might not be; things should still work normally either way.
  ;;
  ;; Special Optimization 1: if `instance` is an `IInstance`, and original and current are the same object, this only
  ;; applies `xform` once.
  (instance/update-original-and-current
   instance
   (fn [row]
     (assert (map? row)
             (format "%s expected map rows, got %s" `apply-row-transform (u/safe-pr-str row)))
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
       (u/try-with-error-context ["transform row" {::k k, ::xform xform, ::row row}]
         (doto (update row k xform)
           ((fn [row]
              (assert (map? row)
                      (format "%s: expected row transform function to return a map, got %s"
                              `apply-row-transform (u/safe-pr-str row)))))))))))

(defn- out-transforms [model]
  (wrapped-transforms model :out))

(defn- apply-row-transform-fn [k xform]
  (fn [instance]
    (cond-> instance
      (contains? instance k) (apply-row-transform k xform))))

(defn- row-transform-fn [k->transform]
  {:pre [(map? k->transform) (seq k->transform)]}
  (reduce
   (fn [f [k xform]]
     (comp (apply-row-transform-fn k xform)
      f))
   identity
   k->transform))

;;; HACK this shouldn't be public but we need it for [[toucan.models/post-insert]] for now.
(defn transform-result-rows-transducer
  "Return a transducer to transform rows of `model` using its [[out-transforms]]."
  [model]
  (if-let [k->transform (not-empty (out-transforms model))]
    (map (let [f (row-transform-fn k->transform)]
           (fn [row]
             (u/with-debug-result ["Transform %s row %s" model row]
               (u/try-with-error-context ["transform row" {::model model, ::row row}]
                 (f row))))))
    identity))

(m/defmethod op/reducible-returning-instances* :after [:toucan2.select/select ::transformed]
  [_query-type model reducible-query]
  (eduction
   (transform-result-rows-transducer model)
   reducible-query))

(m/defmethod query/build :before [::update/update ::transformed :default]
  [_query-type model {:keys [query], :as args}]
  (if-let [k->transform (not-empty (in-transforms model))]
    (let [args (-> args
                   (update :kv-args merge query) ;; TODO -- wtf? I'm 99% sure this is wrong.
                   (dissoc :query))]
      (-> (apply-in-transforms model args)
          (update :changes transform-kv-args k->transform)))
    args))

(defn- transform-insert-rows [[first-row :as rows] k->transform]
  {:pre [(map? first-row) (map? k->transform)]}
  ;; all rows should have the same keys, so we just need to look at the keys in the first row
  (let [row-xforms (for [k     (keys first-row)
                         :let  [xform (get k->transform k)]
                         :when xform]
                     (fn [row]
                       (update row k (fn [v]
                                       (if (some? v)
                                         (xform v)
                                         v)))))
        row-xform  (apply comp row-xforms)]
    (map row-xform rows)))

(m/defmethod op/reducible-update* :before [::insert/insert ::transformed]
  [query-type model parsed-args]
  (assert (isa? model ::transformed))
  (if-let [k->transform (in-transforms model)]
    (u/try-with-error-context ["apply in transforms before inserting rows" {::query-type  query-type
                                                                            ::model       model
                                                                            ::parsed-args parsed-args
                                                                            ::transforms  k->transform}]
      (u/with-debug-result ["Apply %s transforms to %s" k->transform parsed-args]
        (update parsed-args :rows transform-insert-rows k->transform)))
    parsed-args))

(def ^:dynamic ^:private *already-transforming-insert-results*
  "This is used to tell the [[op/reducible-update-returning-pks*]] method for handling insert transforms not to do
  anything if it is getting called inside a [[op/reducible-returning-instances*]] call. If we're inside
  a [[op/reducible-returning-instances*]] call it means we need to use the PK columns to fetch results from the DB and
  we should use them as-is; the [[out-transforms]] for `::select/select` will transform the instances the correct way.
  If we are *not* inside a call to [[op/reducible-returning-instances*]], it means we are expected to return just the
  PKs; those should be transformed."
  false)

(m/defmethod op/reducible-update-returning-pks* :after [::insert/insert ::transformed]
  [_query-type model reducible-results]
  (if *already-transforming-insert-results*
    reducible-results
    (let [pk-keys (model/primary-keys model)]
      (eduction
       ;; 1. convert result PKs to a map of PK key -> value
       (map (fn [pk-or-pks]
              (u/with-debug-result ["convert result PKs %s to map of PK key -> value" pk-or-pks]
                (zipmap pk-keys (if (sequential? pk-or-pks)
                                  pk-or-pks
                                  [pk-or-pks])))))
       ;; 2. transform the PK results using the model's [[out-transforms]]
       (transform-result-rows-transducer model)
       ;; 3. Now flatten the maps of PK key -> value back into plain PK values or vectors of plain PK values (if the
       ;; model has a composite primary key)
       (map (let [f (if (= (count pk-keys) 1)
                      (first pk-keys)
                      (juxt pk-keys))]
              (fn [row]
                (u/with-debug-result "convert PKs map back to flat PKs"
                  (f row)))))
       reducible-results))))

;;; `insert-returning-instances!` doesn't need any special transformations, because `select` will handle it -- see
;;; docstring for [[*already-transforming-insert-results*]]
;;;
;;; TODO -- this is busted if we are actually returning the instances directly e.g. with a special identity
;;; implementation of [[op/reducible-returning-instances*]]
;;;
;;; TODO -- does this NEED to be an `:around` method?
(m/defmethod op/reducible-returning-instances* :around [::insert/insert ::transformed]
  [query-type model reducible-results]
  (binding [*already-transforming-insert-results* true]
    (next-method query-type model reducible-results)))


;;;; [[deftransforms]]

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
  (m/prefer-method! transforms ::user-with-location ::user-with-password)
  ```

  If you want to override transforms completely for a model, and ignore transforms from ancestors of a model, you can
  create an `:around` method:

  ```clj
  (defmethod toucan2.tools.transforms :around ::my-model
    [_model]
    {:field {:in name, :out keyword}})
  ```"
  {:style/indent 1}
  [model column->direction->fn]
  `(let [model# ~model]
     (u/maybe-derive model# ::transformed)
     (m/defmethod transforms model#
       [~'&model]
       ~column->direction->fn)))

(s/fdef deftransforms
  :args (s/cat :model      some?
               :transforms any?)
  :ret any?)
