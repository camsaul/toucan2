(ns toucan2.tools.transformed
  (:require
   [better-cond.core :as b]
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [methodical.impl.combo.operator :as m.combo.operator]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.magic-map :as magic-map]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

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
           :arglists '([model₁])}
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
                 (if (or (sequential? v)
                         (set? v))
                   (mapv xform* v)
                   (xform v))))
          (rest v))

    ;; only apply xform if the value is non-nil.
    (some? v)
    (xform v)

    :else
    nil))

(defn- wrapped-transforms
  "Get the [[transforms]] functions for a model in a either the `:in` or `:out` direction; wrap the functions in
  `try-catch` forms so we can meaningful error messages if they fail, and so that the transform is skipped for `nil`
  values."
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
                       (if-not (some? v)
                         v
                         (u/try-with-error-context ["apply transform" {::transforms k->direction->transform
                                                                       ::model      model
                                                                       ::k          k
                                                                       ::v          v
                                                                       ::xform      xform}]
                           (xform v))))]))))))

(defn- in-transforms [model]
  (wrapped-transforms model :in))

;;; this is just here so we can intercept low-level calls to [[query/apply-kv-arg]] instead of needing to know how to
;;; handle stuff like `:toucan/pk` ourselves
;;;
;;; TODO -- this whole thing is still busted a *little* bit I think because it's assuming that everything in `kv-args`
;;; is a CONDITION (goes in a WHERE clause) but I guess if we're intercepting the bottom-level calls to
;;; [[query/apply-kv-arg]] at that point it IS a condition. We need to verify this tho and test it with some sort of
;;; thing like the custom `::limit` thing
;;;
;;; TODO This name is WACK
(defrecord ^:no-doc RecordTypeForInterceptingApplyKVArgCalls [])

(m/defmethod query/apply-kv-arg [#_model :default
                                 #_query RecordTypeForInterceptingApplyKVArgCalls
                                 #_k     :default]
  [model _query k v]
  (let [v (if-let [xform (get (in-transforms model) k)]
            (transform-condition-value xform v)
            v)]
    [k v]))

(def ^:private ^:dynamic *already-transformed* false)

(m/defmethod query/apply-kv-arg [#_model ::transformed.model
                                 #_query :default
                                 #_k     :default]
  [model query k v]
  (if (or (instance? RecordTypeForInterceptingApplyKVArgCalls query)
          *already-transformed*
          (nil? v))
    (next-method model query k v)
    (let [[k v*] (query/apply-kv-arg model (->RecordTypeForInterceptingApplyKVArgCalls) k v)]
      #_(printf "Intercepted apply-kv-arg %s %s => %s\n" k (pr-str v) (pr-str v*))
      (binding [*already-transformed* true]
        (next-method model query k v*)))))

;;;; after select (or other things returning instances)

(defn- apply-result-row-transform [instance k xform]
  (assert (and (map? instance)
               (not (magic-map/magic-map? instance)))
          (format "%s expected map rows, got %s" `apply-result-row-transform (pr-str instance)))
  ;; The "Special Optimizations" below *should* be the default case, but if some other aux methods are in place or
  ;; custom impls it might not be; things should still work normally either way.
  ;;
  ;; Special Optimization 1: if `instance` is an `IInstance`, and original and current are the same object, this only
  ;; applies `xform` once.
  (instance/update-original-and-current
   instance
   (fn [row]
     ;; Special Optimization 2: if the underlying original/current maps of `instance` are instances of something
     ;; like [[toucan2.jdbc.row/->TransientRow]] we can do a 'deferred update' that only applies the transform if and
     ;; when the value is realized.
     (log/tracef :results "Transform %s %s" k (get row k))
     (u/try-with-error-context ["Transform result column" {::k k, ::xform xform, ::row row}]
       (protocols/deferrable-update row k xform)))))

(defn- out-transforms [model]
  (wrapped-transforms model :out))

(defn- apply-result-row-transform-fn
  "Given a `column` and a transform function `xform`, return a function with the signature

  ```clj
  (f row)
  ```

  that will apply that transform to that column if the row contains that column."
  [column xform]
  (fn [instance]
    (cond-> instance
      (contains? instance column) (apply-result-row-transform column xform))))

(defn- result-row-transform-fn
  "Given a map of column key -> transform function, return a function with the signature

  ```clj
  (f row)
  ```

  That can be called on each instance returned in the results."
  [k->transform]
  {:pre [(map? k->transform) (seq k->transform)]}
  (reduce
   (fn [f [k xform]]
     (comp (apply-result-row-transform-fn k xform)
      f))
   identity
   k->transform))

(defn- transform-result-rows-transducer
  "Return a transducer to transform rows of `model` using its [[out-transforms]]."
  [model]
  (if-let [k->transform (not-empty (out-transforms model))]
    (map (let [f (result-row-transform-fn k->transform)]
           (fn [row]
             (assert (map? row) (format "Expected map row, got ^%s %s" (some-> row class .getCanonicalName) (pr-str row)))
             (log/tracef :results "Transform %s row %s" model row)
             (let [result (u/try-with-error-context ["transform result row" {::model model, ::row row}]
                            (f row))]
               (log/tracef :results "[transform] => %s" result)
               result))))
    identity))

(m/defmethod pipeline/transduce-with-model [#_query-type :toucan.result-type/instances #_model ::transformed.model]
  [rf query-type model parsed-args]
  ;; don't try to transform stuff when we're doing SELECT directly with PKs (e.g. to fake INSERT returning instances),
  ;; we're not doing transforms on the way out so we don't need to do them on the way in
  (if (isa? query-type ::pipeline/select.instances-from-pks)
    (binding [*already-transformed* true]
      (next-method rf query-type model parsed-args))
    (let [rf* ((transform-result-rows-transducer model) rf)]
      (next-method rf* query-type model parsed-args))))

;;;; before update

(defn- transform-update-changes [m k->transform]
  {:pre [(map? k->transform) (seq k->transform)]}
  (into {} (for [[k v] m]
             [k (when (some? v)
                  (if-let [xform (get k->transform k)]
                    (xform v)
                    v))])))

(m/defmethod pipeline/transduce-with-model :before [#_query-type :toucan.query-type/update.* #_model ::transformed.model]
  [_rf _query-type model {:keys [changes], :as parsed-args}]
  (b/cond
    (not (map? changes))
    parsed-args

    :let [k->transform (not-empty (in-transforms model))]

    (not k->transform)
    parsed-args

    (update parsed-args :changes transform-update-changes k->transform)))

;;;; before insert

;;; TODO -- this shares a lot of code with [[transform-update-changes]]
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

(m/defmethod pipeline/transduce-with-model :before [#_query-type :toucan.query-type/insert.* #_model ::transformed.model]
  [_rf query-type model parsed-args]
  (assert (isa? model ::transformed.model))
  (b/cond
    (::already-transformed? parsed-args)
    parsed-args

    :let [k->transform (in-transforms model)]

    (empty? k->transform)
    parsed-args

    :else
    (u/try-with-error-context ["apply in transforms before inserting rows" {::query-type  query-type
                                                                            ::model       model
                                                                            ::parsed-args parsed-args
                                                                            ::transforms  k->transform}]
      (log/debugf :compile "Apply %s transforms to %s" k->transform parsed-args)
      (-> parsed-args
          (update :rows transform-insert-rows k->transform)
          (assoc ::already-transformed? true)))))

;;;; after insert

(m/defmethod pipeline/transduce-with-model [#_query-type :toucan.query-type/insert.pks #_model ::transformed.model]
  [rf query-type model parsed-args]
  (let [pk-keys (model/primary-keys model)
        rf*     ((comp
                  ;; 1. convert result PKs to a map of PK key -> value
                  (map (fn [pk-or-pks]
                         (log/tracef :compile "convert result PKs %s to map of PK key -> value" pk-or-pks)
                         (let [m (zipmap pk-keys (if (sequential? pk-or-pks)
                                                   pk-or-pks
                                                   [pk-or-pks]))]
                           (log/tracef :compile "=> %s" pk-or-pks)
                           m)))
                  ;; 2. transform the PK results using the model's [[out-transforms]]
                  (transform-result-rows-transducer model)
                  ;; 3. Now flatten the maps of PK key -> value back into plain PK values or vectors of plain PK values
                  ;; (if the model has a composite primary key)
                  (map (let [f (if (= (count pk-keys) 1)
                                 (first pk-keys)
                                 (juxt pk-keys))]
                         (fn [row]
                           (log/tracef :results "convert PKs map back to flat PKs")
                           (let [row (f row)]
                             (log/tracef :results "=> %s" row)
                             row)))))
                 rf)]
    (next-method rf* query-type model parsed-args)))


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
     (u/maybe-derive model# ::transformed.model)
     (m/defmethod transforms model#
       [~'model]
       ~column->direction->fn)))

(s/fdef deftransforms
  :args (s/cat :model      some?
               :transforms any?)
  :ret any?)
