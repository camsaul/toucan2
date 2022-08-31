(ns toucan2.tools.hydrate
  "[[hydrate]] adds one or more keys to an instance or instances using various hydration strategies, usually using one of
  the existing keys in those instances. A typical use case would be to take a sequence of `orders` and add `:user` keys
  to them based on their values of the foreign key `:user-id`.

  [[hydrate]] is how you *use* the hydration facilities; everything else in this namespace is only for extending
  hydration to support your models.

  Toucan 2 ships with several hydration strategies out of the box:

  #### Automagic Batched Hydration (via [[model-for-automagic-hydration]])

  [[hydrate]] attempts to do a *batched hydration* where possible. If the key being hydrated is defined as one of some
  table's [[model-for-automagic-hydration]], `hydrate` will do a batched
  [[toucan2.select/select]] if a corresponding key (by default, the same key suffixed by `-id`) is found in the
  objects being batch hydrated. The corresponding key can be customized by
  implementing [[fk-keys-for-automagic-hydration]].

  ```clj
  (hydrate [{:user_id 100}, {:user_id 101}] :user)
  ```

  Since `:user` is a hydration key for `:models/User`, a single [[toucan2.select/select]] will used to fetch Users:

  ```clj
  (db/select :models/User :id [:in #{100 101}])
  ```

  The corresponding Users are then added under the key `:user`.

  #### Function-Based Batched Hydration (via [[batched-hydrate]] methods)

  If the key can't be hydrated auto-magically with the appropriate [[model-for-automagic-hydration]],
  [[hydrate]] will attempt to do batched hydration if it can find a matching method
  for [[batched-hydrate]]. If a matching function is found, it is called with a collection of
  objects, e.g.

  ```clj
  (m/defmethod hydrate/batched-hydrate [:default :fields]
    [_model _k rows]
    (let [id->fields (get-some-fields rows)]
      (for [row rows]
        (assoc row :fields (get id->fields (:id row))))))
  ```

  #### Simple Hydration (via [[simple-hydrate]] methods)

  If the key is *not* eligible for batched hydration, [[hydrate]] will look for a matching
  [[simple-hydrate]] method. `simple-hydrate` is called with a single row.

  ```clj
  (m/defmethod simple-hydrate [:default :dashboard]
    [_model _k {:keys [dashboard-id], :as row}]
    (assoc row :dashboard (select/select-one :models/Dashboard :toucan/pk dashboard-id)))
  ```

  #### Hydrating Multiple Keys

  You can hydrate several keys at one time:

  ```clj
  (hydrate {...} :a :b)
    -> {:a 1, :b 2}
  ```

  #### Nested Hydration

  You can do recursive hydration by listing keys inside a vector:

  ```clj
  (hydrate {...} [:a :b])
    -> {:a {:b 1}}
  ```

  The first key in a vector will be hydrated normally, and any subsequent keys will be hydrated *inside* the
  corresponding values for that key.

  ```clj
  (hydrate {...}
           [:a [:b :c] :e])
    -> {:a {:b {:c 1} :e 2}}
  ```

  ### Flowchart

  If you're digging in to the details, this is a flowchart of how hydration works:

  ```
                        hydrate <-------------+
                          |                   |
                      hydrate-forms           |
                          | (for each form)   |
                      hydrate-one-form        | (recursively)
                          |                   |
               keyword? --+-- sequence?       |
                  |             |             |
            hydrate-key   hydrate-key-seq ----+
                  |
         (for each strategy) <--------+
          ::automagic-batched         |
          ::multimethod-batched       |
          ::multimethod-simple        | (try next strategy)
                  |                   |
         can-hydrate-with-strategy?   |
                  |                   |
           yes ---+--- no ------------+
            |
   hydrate-with-strategy
  ```"
  (:require
   [camel-snake-kebab.core :as csk]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(m/defmulti can-hydrate-with-strategy?
  "Can we hydrate the key `k` in instances of `model` using a specific hydration `strategy`?

  Normally you should never need to call this yourself. The only reason you would implement it is if you are
  implementing a custom hydration strategy."
  {:arglists '([model₁ strategy₂ k₃])}
  u/dispatch-on-first-three-args)

(m/defmulti hydrate-with-strategy
  "Hydrate the key `k` in `instances` of `model` using a specific hydration `strategy`.

  Normally you should not call this yourself. The only reason you would implement this method is if you are implementing
  a custom hydration strategy."
  {:arglists '([model strategy₁ k instances])}
  (fn [_model strategy _k _instances]
    strategy))

;;;                                  Automagic Batched Hydration (via :table-keys)
;;; ==================================================================================================================

;;; TODO -- should this get called with `instance` rather than the instance's model? Should we support multiple possible
;;; models automagically hydrating the same key? For example an Emitter in Metabase can be a CardEmitter, that points to
;;; a Card, or a DashboardEmitter, that points to a Dashboard -- would it be possible to hydrate the same key in a
;;; sequence of mixed-type emitters?
(m/defmulti model-for-automagic-hydration
  "The model that should be used to automagically hydrate the key `k` in instances of `original-model`.

  ```clj
  (model-for-automagic-hydration :some-table :user) :-> :myapp.models/user
  ```

  Dispatches off of the [[toucan2.protocols/model]] of the instance being hydrated and the key `k` that we are
  attempting to hydrate. To hydrate the key `k` for *any* model, you can use `:default` in your `defmethod` dispatch
  value. Example implementation:

  ```clj
   ;; when hydrating the :user key for *any* model, hydrate with instances of the model :models/user
   ;;
   ;; By default, this will look for values of :user-id in the instances being hydrated and then fetch the instances of
   ;; :models/user with a matching :id
   (m/defmethod hydrate/model-for-automagic-hydration [:default :user]
     [_original-model _k]
     :models/user)

   ;; when hydrating the :user key for instances of :models/orders, hydrate with instances of :models/user
   (m/defmethod hydrate/model-for-automagic-hydration [:models/orders :user]
     [_original-model _k]
     :models/user)
  ```

  Automagic hydration looks for the [[fk-keys-for-automagic-hydration]] in the instance being hydrated, and if they're
  all non-`nil`, fetches instances of [[model-for-automagic-hydration]] with the
  corresponding [[toucan2.model/primary-keys]]. Thus you might also want to implement

  * [[fk-keys-for-automagic-hydration]] for the model whose instances hydrating and the key being hydrated
    (e.g. `:models/orders` and `:user`)

  * [[toucan2.model/primary-keys]] for the model you want to hydrate with (e.g. `:models/user`)

  #### Tips

  You probably don't want to write an implementation for `[<some-model> :default]` or `[:default :default]`, unless you
  want every key that we attempt to hydrate to be hydrated by that method."
  {:arglists '([original-model₁ k₂])}
  u/dispatch-on-first-two-args)

(m/defmethod model-for-automagic-hydration :default
  [_model _k]
  nil)

(m/defmethod can-hydrate-with-strategy? [:default ::automagic-batched :default]
  [model _strategy dest-key]
  (boolean (model-for-automagic-hydration model dest-key)))

(m/defmulti fk-keys-for-automagic-hydration
  "The keys in we should use when automagically hydrating the key `dest-key` in instances of `original-model` with
  instances `hydrated-model`.

  ```clj
  ;; when hydrating :user in an order with a user, fetch the user based on the value of the :user-id key
  ;;
  ;; This means we take the value of :user-id from our order and then fetch the user with the matching primary key
  ;; (by default, :id)
  (fk-keys-for-automagic-hydration :models/orders :user :models/user) => [:user-id]
  ```

  The model that we are hydrating with (e.g. `:models/user`) is determined by [[model-for-automagic-hydration]].

  By default [[fk-keys-for-automagic-hydration]] is just the key we're hydrating, with `-id` appended to it; e.g. if
  we're hydrating `:user` the default FK key to use is `:user-id`. *If this convention is fine, you do not need to
  implement this method.* If you want to do something that does not follow this convention, like hydrate a `:user` key
  based on values of `:creator-id`, you should implement this method.

  Example implementation:

  ```clj
  ;; hydrate orders :user key using values of :creator-id
  (m/defmethod hydrate/fk-keys-for-automagic-hydration [:model/orders :user :default]
    [_original-model _dest-key _hydrated-model]
    [:creator-id])
  ```

  #### Tips

  When implementing this method, you probably do not want to specialize on the `hydrated-model` (the third part of the
  dispatch value) -- the model to use is normally determined by [[model-for-automagic-hydration]]. The only time you
  might want to specialize on `hydrated-model` is you wanted to do something like

  ```clj
  ;; by default when we hydrate the :user key with a :models/user, use :creator-id as the source FK
  (m/defmethod hydrate/fk-keys-for-automagic-hydration [:default :model/orders :user :models/user]
    [_original-model _dest-key _hydrated-model]
    [:creator-id])
  ```"
  {:arglists '([original-model₁ dest-key₂ hydrated-model₃])}
  u/dispatch-on-first-three-args)

(m/defmethod fk-keys-for-automagic-hydration :default
  [_original-model dest-key _hydrated-key]
  ;; TODO -- this should probably use the key transform associated with the `original-model` -- if it's not using magic
  ;; maps this wouldn't work
  [(csk/->kebab-case (keyword (str (name dest-key) "-id")))])

(m/defmethod fk-keys-for-automagic-hydration :around :default
  [original-model dest-key hydrated-model]
  (assert (keyword? dest-key) "dest-key should be a keyword")
  (let [result (next-method original-model dest-key hydrated-model)]
    (when-not (and (sequential? result)
                   (seq result)
                   (every? keyword? result))
      (throw (ex-info (format "fk-keys-for-automagic-hydration should return a non-empty sequence of keywords. Got: %s" (u/safe-pr-str result))
                      {:original-model original-model
                       :dest-key       dest-key
                       :hydrated-model hydrated-model
                       :result         result})))
    result))

(defn- automagic-batched-hydration-add-fks [dest-key results fk-keys]
  (assert (seq fk-keys) "fk-keys cannot be empty")
  (let [get-fk-values (apply juxt fk-keys)]
    (for [row results]
      (if (get row dest-key)
        (do
          (u/println-debug ["Don't need to hydrate %s: already has %s" row dest-key])
          row)
        (let [fk-vals (get-fk-values row)]
          (if (every? some? fk-vals)
            (do
              (u/println-debug ["Attempting to hydrate %s with values of %s %s" row fk-keys fk-vals])
              (assoc row ::fk fk-vals))
            (do
              (u/println-debug ["Skipping %s: values of %s are %s" row fk-keys fk-vals])
              row)))))))

(defn- automagic-batched-hydration-fetch-pk->instance [hydrating-model rows]
  (let [pk-keys (model/primary-keys hydrating-model)]
    (assert (pos? (count pk-keys)))
    (if-let [fk-values-set (not-empty (set (filter some? (map ::fk rows))))]
      (let [query {:where (let [clauses (map-indexed
                                         (fn [i pk-key]
                                           [:in pk-key (set (map #(nth % i) fk-values-set))])
                                         pk-keys)]
                            (if (> (count clauses) 1)
                              (cons :and clauses)
                              (first clauses)))}]
        (u/with-debug-result ["Fetching %s with PKs %s %s" hydrating-model pk-keys query]
          (select/select-pk->fn realize/realize hydrating-model query)))
      (u/println-debug ["Not hydrating %s because no rows have non-nil FK values" hydrating-model]))))

(defn- do-automagic-batched-hydration [dest-key rows pk->fetched-instance]
  (u/with-debug-result ["Attempting to hydrate %d/%d rows" (count (filter ::fk rows)) (count rows)]
    (for [row rows]
      (if-not (::fk row)
        row
        (let [fk-vals          (::fk row)
              ;; convert fk to from [id] to id if it only has one key. This is what `select-pk->fn` returns.
              fk-vals          (if (= (count fk-vals) 1)
                                 (first fk-vals)
                                 fk-vals)
              fetched-instance (get pk->fetched-instance fk-vals)]
          (u/with-debug-result ["Hydrate %s %s with %s" dest-key [row] (or fetched-instance
                                                                           "nil (no matching fetched row)")]
            (cond-> (dissoc row ::fk)
              fetched-instance (assoc dest-key fetched-instance))))))))

(m/defmethod hydrate-with-strategy ::automagic-batched
  [model _strategy dest-key rows]
  (u/try-with-error-context ["automagic batched hydration" {::model model, ::dest-key dest-key}]
    (let [hydrating-model      (model-for-automagic-hydration model dest-key)
          _                    (u/println-debug ["Hydrating %s key %s with rows from %s" (or model "map") dest-key hydrating-model])
          fk-keys              (fk-keys-for-automagic-hydration model dest-key hydrating-model)
          _                    (u/println-debug ["Hydrating with FKs %s" fk-keys])
          rows                 (automagic-batched-hydration-add-fks dest-key rows fk-keys)
          pk->fetched-instance (automagic-batched-hydration-fetch-pk->instance hydrating-model rows)]
      (u/println-debug ["Fetched %d rows of %s" (count pk->fetched-instance) hydrating-model])
      (do-automagic-batched-hydration dest-key rows pk->fetched-instance))))


;;;                         Method-Based Batched Hydration (using impls of [[batched-hydrate]])
;;; ==================================================================================================================

(m/defmulti batched-hydrate
  "Hydrate the key `k` in one or more `instances` of `model`. Implement this method to support batched hydration for the
  key `k`. Example implementation:

  ```clj
  ;; This method defines a batched hydration strategy for the :bird-type key for all models.
  (m/defmethod hydrate/batched-hydrate [:default :is-bird?]
    [_model _k rows]
    ;; fetch a set of all the non-nil bird IDs in rows.
    (let [bird-ids           (into #{} (comp (map :bird-id) (filter some?)) rows)
          ;; if bird-ids is non-empty, fetch a map of bird ID => bird type
          bird-id->bird-type (when (seq bird-ids)
                               (select/select-pk->fn :bird-type :models/bird :id [:in bird-ids]))]
      ;; for each row add a :bird-type key.
      (for [row rows]
        (assoc row :bird-type (get bird-id->bird-type (:bird-id row))))))
  ```

  Batched hydration implementations should try to be efficient, e.g. minimizing the number of database calls made rather
  than doing one call per row. If you just want to hydrate each row independently, implement [[simple-hydrate]] instead.
  If you are hydrating entire instances of some other model, consider setting up automagic batched hydration
  using [[model-for-automagic-hydration]] and possibly [[fk-keys-for-automagic-hydration]]."
  {:arglists '([model₁ k₂ instances])}
  u/dispatch-on-first-two-args)

(m/defmethod can-hydrate-with-strategy? [:default ::multimethod-batched :default]
  [model _strategy k]
  (boolean (m/effective-primary-method batched-hydrate (m/dispatch-value batched-hydrate model k))))

(m/defmethod hydrate-with-strategy ::multimethod-batched
  [model _strategy k instances]
  (batched-hydrate model k instances))


;;;                          Method-Based Simple Hydration (using impls of [[simple-hydrate]])
;;; ==================================================================================================================

;;; TODO -- better dox
(m/defmulti simple-hydrate
  "Implementations should return a version of map `row` with the key `k` added."
  {:arglists '([model₁ k₂ row])}
  u/dispatch-on-first-two-args)

(m/defmethod simple-hydrate :around :default
  [model k row]
  (u/try-with-error-context ["simple hydrate" {:model model, :k k, :row row}]
    (next-method model k row)))

(m/defmethod can-hydrate-with-strategy? [:default ::multimethod-simple :default]
  [model _strategy k]
  (boolean (m/effective-primary-method simple-hydrate (m/dispatch-value simple-hydrate model k))))

(m/defmethod hydrate-with-strategy ::multimethod-simple
  [model _strategy k rows]
  ;; TODO -- consider whether we should optimize this a bit and cache the methods we use so we don't have to go thru
  ;; multimethod dispatch on every row.
  (for [row rows]
    (when row
      (simple-hydrate model k row))))


;;;                                           Hydration Using All Strategies
;;; ==================================================================================================================

(defn- strategies []
  (keys (m/primary-methods hydrate-with-strategy)))

(defn ^:no-doc hydration-strategy
  "Determine the appropriate hydration strategy to hydrate the key `k` in instances of `model`."
  [model k]
  (some
   (fn [strategy]
     (when (can-hydrate-with-strategy? model strategy k)
       strategy))
   (strategies)))


;;;                                               Primary Hydration Fns
;;; ==================================================================================================================

(declare hydrate)

(def ^:dynamic *error-on-unknown-key* nil)

(defonce ^:private global-error-on-unknown-key (atom false))

(defn set-error-on-unknown-key! [new-value]
  (reset! global-error-on-unknown-key new-value))

(defn ^:no-doc error-on-unknown-key? []
  (if (some? *error-on-unknown-key*)
    *error-on-unknown-key*
    @global-error-on-unknown-key))

(defn- hydrate-key
  [model rows k]
  (if-let [strategy (hydration-strategy model k)]
    (u/try-with-error-context ["hydrate key" {:model model, :key k, :strategy strategy}]
      (u/with-debug-result ["Hydrating %s %s with strategy %s" (or model "map") k strategy]
        (hydrate-with-strategy model strategy k rows)))
    (do
      (u/println-debug ["Don't know how to hydrate %s for model %s rows %s" k model (take 1 rows)])
      (when (error-on-unknown-key?)
        (throw (ex-info (format "Don't know how to hydrate %s" (pr-str k))
                        {:model model, :rows rows, :k k})))
      rows)))

(defn- hydrate-key-seq
  "Hydrate a nested hydration form (vector) by recursively calling [[hydrate]]."
  [results [k & nested-keys :as coll]]
  (when-not (seq nested-keys)
    (throw (ex-info (str (format "Invalid hydration form: replace %s with %s. Vectors are for nested hydration." coll k)
                         " There's no need to use one when you only have a single key.")
                    {:invalid-form coll})))
  (let [results                     (hydrate results k)
        newly-hydrated-values       (map k results)
        recursively-hydrated-values (apply hydrate newly-hydrated-values nested-keys)]
    (map
     (fn [result v]
       (if (and result (some? v))
         (assoc result k v)
         result))
     results
     recursively-hydrated-values)))

(declare hydrate-one-form)

(defn- flatten-collection
  "Convert a collection `coll` into a flattened sequence of maps with `:item` and `:path`.

  ```clj
  (flatten-collection [[{:a 1}]])
  =>
  [{:path [], :item []}
   {:path [0], :item []}
   {:path [0 0], :item {:a 1}}]
  ```"
  ([coll]
   (flatten-collection [] coll))

  ([path coll]
   (if (sequential? coll)
     (into [{:path path, :item (empty coll)}]
           (comp (map-indexed (fn [i x]
                                (flatten-collection (conj path i) x)))
                 cat)
           coll)
     [{:path path, :item coll}])))

(defn- unflatten-collection
  "Take a sequence of items flattened by [[flatten-collection]] and restore them to their original shape."
  [flattened]
  (reduce
   (fn [acc {:keys [path item]}]
     (if (= path [])
       item
       (assoc-in acc path item)))
   []
   flattened))

(defn- hydrate-sequence-of-sequences [model coll k]
  (let [flattened (flatten-collection coll)
        items     (for [{:keys [item path]} flattened
                        :when (map? item)]
                    (vary-meta item assoc ::path path))
        hydrated (hydrate-one-form model items k)]
    (unflatten-collection (concat flattened
                                  (for [item hydrated]
                                    (do
                                      (assert (::path (meta item)))
                                      {:item item, :path (::path (meta item))}))))))

(defn- hydrate-one-form
  "Hydrate for a single hydration key or form `k`."
  [model results k]
  (u/println-debug ["hydrate %s for model %s rows %s" k model (take 1 results)])
  (cond
    (and (sequential? results)
         (empty? results))
    results

    (sequential? (first results))
    (hydrate-sequence-of-sequences model results k)

    (keyword? k)
    (hydrate-key model results k)

    (sequential? k)
    (hydrate-key-seq results k)

    :else
    (throw (ex-info (format "Invalid hydration form: %s. Expected keyword or sequence." k)
                    {:invalid-form k}))))

(defn- hydrate-forms
  "Hydrate many hydration forms across a *sequence* of `results` by recursively calling [[hydrate-one-form]]."
  [model results & forms]
  (reduce (partial hydrate-one-form model) results forms))

(defn- unnest-first-result
  "Given an arbitrarily nested sequence `coll`, continue unnesting the sequence until we get a non-sequential first item.

  ```clj
  (unnest-first-result [:a :b])  => :a
  (unnest-first-result [[:a]])   => :a
  (unnest-first-result [[[:a]]]) => :a
  ```"
  [coll]
  (->> (iterate first coll)
       (take-while sequential?)
       last
       first))


;;;                                                 Public Interface
;;; ==================================================================================================================

(defn hydrate
  "Hydrate the keys `ks` in a single instance or sequence of instances. See [[toucan2.tools.hydrate]] for more
  information."
  ;; no keys -- no-op
  ([instance-or-instances]
   instance-or-instances)

  ([instance-or-instances & ks]
   (u/try-with-error-context ["hydrate" {:what instance-or-instances, :keys ks}]
     (cond
       (not instance-or-instances)
       nil

       (and (sequential? instance-or-instances)
            (empty? instance-or-instances))
       instance-or-instances

       ;; sequence of instances
       (sequential? instance-or-instances)
       (let [model (protocols/model (unnest-first-result instance-or-instances))]
         (apply hydrate-forms model instance-or-instances ks))

       ;; not sequential
       :else
       (first (apply hydrate-forms (protocols/model instance-or-instances) [instance-or-instances] ks))))))
