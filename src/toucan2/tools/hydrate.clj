(ns toucan2.tools.hydrate
  (:require
   [camel-snake-kebab.core :as csk]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(m/defmulti can-hydrate-with-strategy?
  {:arglists '([model strategy k])}
  u/dispatch-on-first-three-args)

(m/defmulti hydrate-with-strategy
  {:arglists '([model strategy k rows])}
  (fn [_model strategy _k _rows]
    strategy))

;;;                                  Automagic Batched Hydration (via :table-keys)
;;; ==================================================================================================================

;;; TODO - rename this to `model-for-automagic-hydration`
(m/defmulti table-for-automagic-hydration
  "The model that should be used to automagically hydrate the key `k` in instances of `source-model`.

    (table-for-automagic-hydration :some-table :user) :-> :myapp.models/user"
  {:arglists '([source-model k])}
  u/dispatch-on-first-two-args)

(m/defmethod table-for-automagic-hydration :default
  [_model _k]
  nil)

(m/defmethod can-hydrate-with-strategy? [:default ::automagic-batched :default]
  [model _strategy dest-key]
  (boolean (table-for-automagic-hydration model dest-key)))

(m/defmulti fk-keys-for-automagic-hydration
  {:arglists '([original-model dest-key hydrated-model])}
  u/dispatch-on-first-three-args)

(m/defmethod fk-keys-for-automagic-hydration :default
  [_original-model dest-key _hydrated-key]
  [(csk/->kebab-case (keyword (str (name dest-key) "-id")))])

(m/defmethod fk-keys-for-automagic-hydration :around :default
  [original-model dest-key hydrated-model]
  (assert (keyword? dest-key) "dest-key should be a keyword")
  (let [result (next-method original-model dest-key hydrated-model)]
    (when-not (and (sequential? result)
                   (seq result)
                   (every? keyword? result))
      (throw (ex-info (format "fk-keys-for-automagic-hydration should return a non-empty sequence of keywords. Got: %s" (pr-str result))
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
  (u/with-debug-result #_-no-result ["Attempting to hydrate %d/%d rows" (count (filter ::fk rows)) (count rows)]
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
  (try
    (let [hydrating-model      (table-for-automagic-hydration model dest-key)
          _                    (u/println-debug ["Hydrating %s key %s with rows from %s" (or model "map") dest-key hydrating-model])
          fk-keys              (fk-keys-for-automagic-hydration model dest-key hydrating-model)
          _                    (u/println-debug ["Hydrating with FKs %s" fk-keys])
          rows                 (automagic-batched-hydration-add-fks dest-key rows fk-keys)
          pk->fetched-instance (automagic-batched-hydration-fetch-pk->instance hydrating-model rows)]
      (u/println-debug ["Fetched %d rows of %s" (count pk->fetched-instance) hydrating-model])
      (do-automagic-batched-hydration dest-key rows pk->fetched-instance))
    (catch Throwable e
      (throw (ex-info (format "Error doing automagic batched hydration: %s" (ex-message e))
                      {:model model
                       :dest-key  dest-key}
                      e)))))


;;;                         Method-Based Batched Hydration (using impls of `batched-hydrate`)
;;; ==================================================================================================================

(m/defmulti batched-hydrate
  {:arglists '([model k rows])}
  u/dispatch-on-first-two-args)

(m/defmethod can-hydrate-with-strategy? [:default ::multimethod-batched :default]
  [model _strategy k]
  (boolean (m/effective-primary-method batched-hydrate (m/dispatch-value batched-hydrate model k))))

(m/defmethod hydrate-with-strategy ::multimethod-batched
  [model _strategy k rows]
  (batched-hydrate model k rows))


;;;                          Method-Based Simple Hydration (using impls of [[simple-hydrate]])
;;; ==================================================================================================================

(declare simple-hydrate)

(m/defmulti simple-hydrate
  "Implementations should return a version of map `row` with the key `k` added."
  {:arglists '([model k row])}
  u/dispatch-on-first-two-args)

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

(defn hydration-strategy [model k]
  (some
   (fn [strategy]
     (when (can-hydrate-with-strategy? model strategy k)
       strategy))
   (strategies)))


;;;                                               Primary Hydration Fns
;;; ==================================================================================================================

(declare hydrate)

(defn- hydrate-key
  [model rows k]
  (if-let [strategy (hydration-strategy model k)]
    (u/with-debug-result ["Hydrating %s %s with strategy %s" (or model "map") k strategy]
      (hydrate-with-strategy model strategy k rows))
    (do
      (u/println-debug ["Don't know how to hydrate %s" k])
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

(defn- hydrate-sequence-of-sequences [model groups k]
  (let [indexed-flattened-results (for [[i group] (map-indexed vector groups)
                                        result    group]
                                    [i result])
        indecies                  (map first indexed-flattened-results)
        flattened                 (map second indexed-flattened-results)
        hydrated                  (hydrate-one-form model flattened k)
        groups                    (partition-by first (map vector indecies hydrated))]
    (for [group groups]
      (map second group))))

(defn- hydrate-one-form
  "Hydrate a single hydration form."
  [model results k]
  (when (seq results)
    (if (sequential? (first results))
      (hydrate-sequence-of-sequences model results k)
      (cond
        (keyword? k)
        (hydrate-key model results k)

        (sequential? k)
        (hydrate-key-seq results k)

        :else
        (throw (ex-info (format "Invalid hydration form: %s. Expected keyword or sequence." k) {:invalid-form k}))))))

(defn- hydrate-forms
  "Hydrate many hydration forms across a *sequence* of `results` by recursively calling `hydrate-one-form`."
  [model results & forms]
  (reduce (partial hydrate-one-form model) results forms))


;;;                                                 Public Interface
;;; ==================================================================================================================

;;                         hydrate <-------------+
;;                           |                   |
;;                       hydrate-forms           |
;;                           | (for each form)   |
;;                       hydrate-one-form        | (recursively)
;;                           |                   |
;;                keyword? --+-- sequence?       |
;;                   |             |             |
;;             hydrate-key   hydrate-key-seq ----+
;;                   |
;;          (for each strategy) <--------+
;;           ::automagic-batched         |
;;           ::multimethod-batched       |
;;           ::multimethod-simple        | (try next strategy)
;;                   |                   |
;;          can-hydrate-with-strategy?   |
;;                   |                   |
;;            yes ---+--- no ------------+
;;             |
;;    hydrate-with-strategy


(defn hydrate
  "Hydrate a single object or sequence of objects.

  #### Automagic Batched Hydration (via [[toucan2.hydrate/table-for-automagic-hydration]])

  [[toucan2.hydrate/hydrate]] attempts to do a *batched hydration* where possible. If the key being hydrated is
  defined as one of some table's [[toucan2.hydrate/table-for-automagic-hydration]], `hydrate` will do a batched
  [[toucan2.select/select]] if a corresponding key (by default, the same key suffixed by `-id`) is found in the
  objects being batch hydrated. The corresponding key can be customized by
  implementing [[toucan2.hydrate/fk-keys-for-automagic-hydration]].

    (hydrate [{:user_id 100}, {:user_id 101}] :user)

  Since `:user` is a hydration key for `:models/User`, a single [[toucan2.select/select]] will used to fetch Users:

    (db/select :models/User :id [:in #{100 101}])

  The corresponding Users are then added under the key `:user`.

  #### Function-Based Batched Hydration (via [[toucan2.hydrate/batched-hydrate]] methods)

  If the key can't be hydrated auto-magically with the appropriate [[toucan2.hydrate/table-for-automagic-hydration]],
  [[toucan2.hydrate/hydrate]] will attempt to do batched hydration if it can find a matching method
  for [[toucan2.hydrate/batched-hydrate]]. If a matching function is found, it is called with a collection of
  objects, e.g.

    (m/defmethod hydrate/batched-hydrate [:default :fields]
      [_model _k rows]
      (let [id->fields (get-some-fields rows)]
        (for [row rows]
          (assoc row :fields (get id->fields (:id row))))))

  #### Simple Hydration (via [[toucan2.hydrate/simple-hydrate]] methods)

  If the key is *not* eligible for batched hydration, [[toucan2.hydrate/hydrate]] will look for a matching
  [[toucan2.hydrate/simple-hydrate]] method. `simple-hydrate` is called with a single row.

    (m/defmethod simple-hydrate [:default :dashboard]
      [_model _k {:keys [dashboard-id], :as row}]
      (assoc row :dashboard (select/select-one :models/Dashboard :toucan/pk dashboard-id)))

  #### Hydrating Multiple Keys

  You can hydrate several keys at one time:

    (hydrate {...} :a :b)
      -> {:a 1, :b 2}

  #### Nested Hydration

  You can do recursive hydration by listing keys inside a vector:

    (hydrate {...} [:a :b])
      -> {:a {:b 1}}

  The first key in a vector will be hydrated normally, and any subsequent keys will be hydrated *inside* the
  corresponding values for that key.

    (hydrate {...}
             [:a [:b :c] :e])
      -> {:a {:b {:c 1} :e 2}}"
  [results k & ks]
  (when results
    (if (sequential? results)
      (if (empty? results)
        results
        (let [first-row (first results)]
          (apply hydrate-forms (instance/model first-row) results k ks)))
      (first (apply hydrate-forms (instance/model results) [results] k ks)))))
