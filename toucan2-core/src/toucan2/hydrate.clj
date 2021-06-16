(ns toucan2.hydrate
  (:require [camel-snake-kebab.core :as csk]
            [methodical.core :as m]
            [toucan2.instance :as instance]
            [toucan2.log :as log]
            [toucan2.realize :as realize]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

(m/defmulti can-hydrate-with-strategy?*
  {:arglists '([connectableᵈ tableableᵈ strategyᵈ kᵈᵗ])}
  u/dispatch-on-first-four-args)

(m/defmulti hydrate-with-strategy*
  {:arglists '([connectable tableable strategyᵈ k rowsᵗ])}
  (fn [_ _ strategy _ _]
    strategy))

;;;                                  Automagic Batched Hydration (via :table-keys)
;;; ==================================================================================================================

(m/defmulti table-for-automagic-hydration*
  "The table that should be used to automagically hydrate from based on values of `k`.

    (table-for-automagic-hydration* :toucan2/default :some-table :user) :-> :myapp.models/user"
  {:arglists '([connectableᵈ tableableᵈ kᵈᵗ])}
  u/dispatch-on-first-three-args)

(m/defmethod table-for-automagic-hydration* :default
  [_ _ _]
  nil)

(m/defmethod can-hydrate-with-strategy?* [:default :default ::automagic-batched :default]
  [connectable tableable strategy dest-key]
  (boolean (table-for-automagic-hydration* connectable tableable dest-key)))

(m/defmulti fk-keys-for-automagic-hydration*
  {:arglists '([connectableᵈ original-tableableᵈ dest-keyᵈ hydrated-tableableᵈᵗ])}
  u/dispatch-on-first-four-args)

(m/defmethod fk-keys-for-automagic-hydration* :default
  [_ _ dest-key _]
  [(csk/->kebab-case (keyword (str (name dest-key) "-id")))])

(m/defmethod fk-keys-for-automagic-hydration* :around :default
  [connectable original-tableable dest-key hydrated-tableable]
  (assert (keyword? dest-key) "dest-key should be a keyword")
  (let [result (next-method connectable original-tableable dest-key hydrated-tableable)]
    (when-not (and (sequential? result)
                   (seq result)
                   (every? keyword? result))
      (throw (ex-info (format "fk-keys-for-automagic-hydration* should return a non-empty sequence of keywords. Got: %s" (pr-str result))
                      ;; TODO -- include connectable in the error if *include-connectable-in-error* is enabled
                      ;; once we add something like that
                      {#_:connectable        #_connectable
                       :original-tableable original-tableable
                       :dest-key           dest-key
                       :hydrated-tableable hydrated-tableable
                       :result             result})))
    result))

(defn- automagic-batched-hydration-add-fks [dest-key results fk-keys]
  (assert (seq fk-keys) "fk-keys cannot be empty")
  (let [get-fk-values (apply juxt fk-keys)]
    (for [row results]
      (if (get row dest-key)
        (do
          (log/tracef "Don't need to hydrate %s: already has %s" row dest-key)
          row)
        (let [fk-vals (get-fk-values row)]
          (if (every? some? fk-vals)
            (do
              (log/tracef "Attempting to hydrate with values of %s %s" fk-keys fk-vals)
              (log/indent-when-debugging
                (log/trace row))
              (assoc row ::fk fk-vals))
            (do
              (log/tracef "Skipping %s: values of %s are %s" row fk-keys fk-vals)
              row)))))))

(defn- automagic-batched-hydration-fetch-pk->instance [connectable hydrating-table rows]
  (let [pk-keys (tableable/primary-key-keys connectable hydrating-table)]
    (assert (pos? (count pk-keys)))
    (if-let [fk-values-set (not-empty (set (filter some? (map ::fk rows))))]
      (let [query {:where (let [clauses (map-indexed
                                         (fn [i pk-key]
                                           [:in pk-key (set (map #(nth % i) fk-values-set))])
                                         pk-keys)]
                            (if (> (count clauses) 1)
                              (cons :and clauses)
                              (first clauses)))}]
        (log/with-trace ["Fetching %s with PKs %s %s" hydrating-table pk-keys query]
          (select/select-pk->fn realize/realize [connectable hydrating-table] query)))
      (log/tracef "Not hydrating %s because no rows have non-nil FK values" hydrating-table))))

(defn- do-automagic-batched-hydration [dest-key rows pk->fetched-instance]
  (log/with-trace-no-result ["Attempting to hydrate %d/%d rows" (count (filter ::fk rows)) (count rows)]
    (for [row rows]
      (if-not (::fk row)
        row
        (let [fk-vals          (::fk row)
              ;; convert fk to from [id] to id if it only has one key. This is what `select-pk->fn` returns.
              fk-vals          (if (= (count fk-vals) 1)
                                 (first fk-vals)
                                 fk-vals)
              fetched-instance (get pk->fetched-instance fk-vals)]
          (log/with-trace ["Hydrate %s" dest-key]
              (log/trace (u/pprint-to-str row))
              (log/trace "with")
              (log/trace (or (some-> fetched-instance u/pprint-to-str) "nil (no matching fetched row)"))
              (cond-> (dissoc row ::fk)
                fetched-instance (assoc dest-key fetched-instance))))))))

(m/defmethod hydrate-with-strategy* ::automagic-batched
  [connectable tableable strategy dest-key rows]
  (try
    (let [hydrating-table      (table-for-automagic-hydration* connectable tableable dest-key)
          _                    (log/tracef "Hydrating %s key %s with rows from %s" (or tableable "map") dest-key hydrating-table)
          fk-keys              (fk-keys-for-automagic-hydration* connectable tableable dest-key hydrating-table)
          _                    (log/tracef "Hydrating with FKs %s" fk-keys)
          rows                 (automagic-batched-hydration-add-fks dest-key rows fk-keys)
          pk->fetched-instance (automagic-batched-hydration-fetch-pk->instance connectable hydrating-table rows)]
      (log/tracef "Fetched %d rows of %s" (count pk->fetched-instance) hydrating-table)
      (do-automagic-batched-hydration dest-key rows pk->fetched-instance))
    (catch Throwable e
      (throw (ex-info (format "Error doing automagic batched hydration: %s" (ex-message e))
                      {:tableable tableable
                       :dest-key  dest-key}
                      e)))))


;;;                         Method-Based Batched Hydration (using impls of `batched-hydrate`*)
;;; ==================================================================================================================

(m/defmulti batched-hydrate*
  {:arglists '([connectableᵈ tableableᵈ kᵈ rowsᵗ])}
  u/dispatch-on-first-three-args)

(m/defmethod can-hydrate-with-strategy?* [:default :default ::multimethod-batched :default]
  [connectable tableable _ k]
  (boolean (m/effective-primary-method batched-hydrate* (m/dispatch-value batched-hydrate* connectable tableable k))))

(m/defmethod hydrate-with-strategy* ::multimethod-batched
  [connectable tableable _ k rows]
  (batched-hydrate* connectable tableable k rows))


;;;                          Method-Based Simple Hydration (using impls of `simple-hydrate*`)
;;; ==================================================================================================================

(declare simple-hydrate*)

(m/defmulti simple-hydrate*
  {:arglists '([connectableᵈ tableableᵈ kᵈ rowᵗ])}
  u/dispatch-on-first-three-args)

(m/defmethod can-hydrate-with-strategy?* [:default :default ::multimethod-simple :default]
  [connectable tableable _ k]
  (boolean (m/effective-primary-method simple-hydrate* (m/dispatch-value simple-hydrate* connectable tableable k))))

(m/defmethod hydrate-with-strategy* ::multimethod-simple
  [connectable tableable _ k rows]
  ;; TODO -- consider whether we should optimize this a bit and cache the methods we use so we don't have to go thru
  ;; multimethod dispatch on every row.
  (for [row rows]
    (when row
      (simple-hydrate* connectable tableable k row))))


;;;                                           Hydration Using All Strategies
;;; ==================================================================================================================

(defn- strategies []
  (keys (m/primary-methods hydrate-with-strategy*)))

(defn hydration-strategy [connectable tableable k]
  (some
   (fn [strategy]
     (when (can-hydrate-with-strategy?* connectable tableable strategy k)
       strategy))
   (strategies)))


;;;                                               Primary Hydration Fns
;;; ==================================================================================================================

(declare hydrate)

(defn- hydrate-key
  [connectable tableable rows k]
  (if-let [strategy (hydration-strategy connectable tableable k)]
    (log/with-trace-no-result ["Hydrating %s %s with strategy %s" (or tableable "map") k strategy]
      (hydrate-with-strategy* connectable tableable strategy k rows))
    (do
      (log/tracef "Don't know how to hydrate %s" k)
      rows)))

(defn- hydrate-key-seq
  "Hydrate a nested hydration form (vector) by recursively calling `hydrate`."
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

(defn- hydrate-sequence-of-sequences [connectable tableable groups k]
  (let [indexed-flattened-results (for [[i group] (map-indexed vector groups)
                                        result    group]
                                    [i result])
        indecies                  (map first indexed-flattened-results)
        flattened                 (map second indexed-flattened-results)
        hydrated                  (hydrate-one-form connectable tableable flattened k)
        groups                    (partition-by first (map vector indecies hydrated))]
    (for [group groups]
      (map second group))))

(defn- hydrate-one-form
  "Hydrate a single hydration form."
  [connectable tableable results k]
  (when (seq results)
    (if (sequential? (first results))
      (hydrate-sequence-of-sequences connectable tableable results k)
      (cond
        (keyword? k)
        (hydrate-key connectable tableable results k)

        (sequential? k)
        (hydrate-key-seq results k)

        :else
        (throw (ex-info (format "Invalid hydration form: %s. Expected keyword or sequence." k) {:invalid-form k}))))))

(defn- hydrate-forms
  "Hydrate many hydration forms across a *sequence* of `results` by recursively calling `hydrate-one-form`."
  [connectable tableable results & forms]
  (reduce (partial hydrate-one-form connectable tableable) results forms))


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
;;          can-hydrate-with-strategy?*   |
;;                   |                   |
;;            yes ---+--- no ------------+
;;             |
;;    hydrate-with-strategy*


(defn hydrate
  "Hydrate a single object or sequence of objects.

  #### Automagic Batched Hydration (via [[toucan2.hydrate/table-for-automagic-hydration*]])

  [[toucan2.hydrate/hydrate]] attempts to do a *batched hydration* where possible. If the key being hydrated is
  defined as one of some table's [[toucan2.hydrate/table-for-automagic-hydration*]], `hydrate` will do a batched
  [[toucan2.select/select]] if a corresponding key (by default, the same key suffixed by `-id`) is found in the
  objects being batch hydrated. The corresponding key can be customized by
  implementing [[toucan2.hydrate/fk-keys-for-automagic-hydration*]].

    (hydrate [{:user_id 100}, {:user_id 101}] :user)

  Since `:user` is a hydration key for `:models/User`, a single [[toucan2.select/select]] will used to fetch Users:

    (db/select :models/User :id [:in #{100 101}])

  The corresponding Users are then added under the key `:user`.

  #### Function-Based Batched Hydration (via [[toucan2.hydrate/batched-hydrate*]] methods)

  If the key can't be hydrated auto-magically with the appropriate [[toucan2.hydrate/table-for-automagic-hydration*]],
  [[toucan2.hydrate/hydrate]] will attempt to do batched hydration if it can find a matching method
  for [[toucan2.hydrate/batched-hydrate*]]. If a matching function is found, it is called with a collection of
  objects, e.g.

    (m/defmethod hydrate/batched-hydrate* [:default :default :fields]
      [_ _ _ tables]
      ...)

    (let [tables (get-some-tables)]
      (hydrate tables :fields))

  #### Simple Hydration (via [[toucan2.hydrate/simple-hydrate*]] methods)

  If the key is *not* eligible for batched hydration, [[toucan2.hydrate/hydrate]] will look for a matching
  [[toucan2.hydrate/simple-hydrate*]] method. `simple-hydrate*` is called with a single row.

    (m/defmethod simple-hydrate* [:default :default :dashboard]
      [_ _ _ {:keys [dashboard-id], :as row}]
      (assoc row :dashboard (select/select-one :models/Dashboard dashboard-id)))

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
          (apply hydrate-forms (instance/connectable first-row) (instance/tableable first-row) results k ks)))
      (first (apply hydrate-forms (instance/connectable results) (instance/tableable results) [results] k ks)))))
