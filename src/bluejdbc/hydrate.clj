(ns bluejdbc.hydrate
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti can-hydrate-with-strategy?
  {:arglists '([strategy results k])}
  u/dispatch-on-first-arg)

(m/defmulti hydrate-with-strategy
  {:arglists '([strategy results k])}
  u/dispatch-on-first-arg)

;;;                                  Automagic Batched Hydration (via :table-keys)
;;; ==================================================================================================================

(m/defmulti hydration-keys
  "The `hydration-keys` method can be overridden to specify the keyword field names that should be hydrated as instances
  of this table. For example, `User` might include `:creator`, which means `hydrate` will look for `:creator_id` or
  `:creator-id` in other objects to find the User ID, and fetch the `Users` corresponding to those values."
  {:arglists '([table])}
  u/dispatch-on-first-arg)

(m/defmulti automagic-hydration-key-table
  {:arglists '([k])}
  identity
  :default-value ::default)

(m/defmethod automagic-hydration-key-table ::default
  [_]
  nil)

(defn- kw-append
  "Append to a keyword.

     (kw-append :user \"_id\") -> :user_id"
  [k suffix]
  (keyword
   (str (when-let [nmspc (namespace k)]
          (str nmspc "/"))
        (name k)
        suffix)))

(m/defmethod can-hydrate-with-strategy? ::automagic-batched
  [_ results k]
  (boolean
   (and (automagic-hydration-key-table k)
        (let [underscore-id-key (kw-append k "_id")
              dash-id-key       (kw-append k "-id")
              contains-k-id?    #(some % [underscore-id-key dash-id-key])]
          (every? contains-k-id? results)))))

(m/defmethod hydrate-with-strategy ::automagic-batched
  [_ results dest-key]
  (let [table       (automagic-hydration-key-table dest-key)
        source-keys #{(kw-append dest-key "_id") (kw-append dest-key "-id")}
        ids         (set (for [result results
                               :when  (not (get result dest-key))
                               :let   [k (some result source-keys)]
                               :when  k]
                           k))
        ;; TODO -- this doesn't work with composite PKs (yet)
        primary-key (tableable/primary-key* nil table)
        objs        (if (seq ids)
                      (into {} (for [item (select/select table, primary-key [:in ids])]
                                 {(primary-key item) item}))
                      (constantly nil))]
    (for [result results
          :let   [source-id (some result source-keys)]]
      (if (get result dest-key)
        result
        (assoc result dest-key (objs source-id))))))


;;;                         Method-Based Batched Hydration (using impls of `batched-hydrate`)
;;; ==================================================================================================================

(m/defmulti batched-hydrate
  {:arglists '([results k])}
  (fn [[first-result] k]
    [(some-> first-result instance/table) k]))

(m/defmethod can-hydrate-with-strategy? ::multimethod-batched
  [_ results k]
  (boolean (m/effective-primary-method batched-hydrate (m/dispatch-value batched-hydrate results k))))

(m/defmethod hydrate-with-strategy ::multimethod-batched
  [_ results k]
  (batched-hydrate results k))


;;;                          Method-Based Simple Hydration (using impls of `simple-hydrate`)
;;; ==================================================================================================================

(declare simple-hydrate)

(m/defmulti simple-hydrate
  {:arglists '([result k])}
  (fn [result k]
    [(some-> result instance/table) k]))

(m/defmethod can-hydrate-with-strategy? ::multimethod-simple
  [_ results k]
  (boolean (m/effective-primary-method simple-hydrate (m/dispatch-value simple-hydrate (first results) k))))

(m/defmethod hydrate-with-strategy ::multimethod-simple
  [_ results k]
  ;; TODO - explain this
  (for [[first-result :as chunk] (partition-by instance/table results)
        :let                     [method (m/effective-primary-method simple-hydrate
                                                                     (m/dispatch-value simple-hydrate first-result k))]
        result                   chunk]
    (when result
      (if (some? (get result k))
        result
        (assoc result k (method result k))))))


;;;                                           Hydration Using All Strategies
;;; ==================================================================================================================

(defn- strategies []
  (keys (m/primary-methods hydrate-with-strategy)))

(defn hydration-strategy [results k]
  (some
   (fn [strategy]
     (when (can-hydrate-with-strategy? strategy results k)
       strategy))
   (strategies)))


;;;                                               Primary Hydration Fns
;;; ==================================================================================================================

(declare hydrate)

(defn- hydrate-key
  [results k]
  (if-let [strategy (hydration-strategy results k)]
    (do
      (log/tracef "Hydrating %s with strategy %s\n" k strategy)
      (hydrate-with-strategy strategy results k))
    results))

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

(defn- hydrate-sequence-of-sequences [groups k]
  (let [indexed-flattened-results (for [[i group] (map-indexed vector groups)
                                        result    group]
                                    [i result])
        indecies                  (map first indexed-flattened-results)
        flattened                 (map second indexed-flattened-results)
        hydrated                  (hydrate-one-form flattened k)
        groups                    (partition-by first (map vector indecies hydrated))]
    (for [group groups]
      (map second group))))

(defn- hydrate-one-form
  "Hydrate a single hydration form."
  [results k]
  (when (seq results)
    (if (sequential? (first results))
      (hydrate-sequence-of-sequences results k)
      (cond
        (keyword? k)
        (hydrate-key results k)

        (sequential? k)
        (hydrate-key-seq results k)

        :else
        (throw (ex-info (format "Invalid hydration form: %s. Expected keyword or sequence." k) {:invalid-form k}))))))

(defn- hydrate-forms
  "Hydrate many hydration forms across a *sequence* of `results` by recursively calling `hydrate-one-form`."
  [results & forms]
  (reduce hydrate-one-form results forms))


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


#### Automagic Batched Hydration (via hydration-keys)

  `hydrate` attempts to do a *batched hydration* where possible.
  If the key being hydrated is defined as one of some table's `hydration-keys`,
  `hydrate` will do a batched `db/select` if a corresponding key ending with `_id`
  is found in the objects being batch hydrated.

    (hydrate [{:user_id 100}, {:user_id 101}] :user)

  Since `:user` is a hydration key for `User`, a single `db/select` will used to
  fetch `Users`:

    (db/select User :id [:in #{100 101}])

  The corresponding `Users` are then added under the key `:user`.


#### Function-Based Batched Hydration (via functions marked ^:batched-hydrate)

  If the key can't be hydrated auto-magically with the appropriate `:hydration-keys`,
  `hydrate` will look for a function tagged with `:batched-hydrate` in its metadata, and
  use that instead. If a matching function is found, it is called with a collection of objects,
  e.g.

    (defn with-fields
      \"Efficiently add `:fields` to a collection of `tables`.\"
      {:batched-hydrate :fields}
      [tables]
      ...)

    (let [tables (get-some-tables)]
      (hydrate tables :fields))     ; uses with-fields

  By default, the function will be used to hydrate keys that match its name; as in the example above,
  you can specify a different key to hydrate for in the metadata instead.


#### Simple Hydration (via functions marked ^:hydrate)

  If the key is *not* eligible for batched hydration, `hydrate` will look for a function or method
  tagged with `:hydrate` in its metadata, and use that instead; if a matching function
  is found, it is called on the object being hydrated and the result is `assoc`ed:

    (defn ^:hydrate dashboard [{:keys [dashboard_id]}]
      (Dashboard dashboard_id))

    (let [dc (DashboardCard ...)]
      (hydrate dc :dashboard))    ; roughly equivalent to (assoc dc :dashboard (dashboard dc))

  As with `:batched-hydrate` functions, by default, the function will be used to hydrate keys that
  match its name; you can specify a different key to hydrate instead as the metadata value of `:hydrate`:

    (defn ^{:hydrate :pk_field} pk-field-id [obj] ...) ; hydrate :pk_field with pk-field-id

  Keep in mind that you can only define a single function/method to hydrate each key; move functions into the
  `IModel` interface as needed.


#### Hydrating Multiple Keys

  You can hydrate several keys at one time:

    (hydrate {...} :a :b)
      -> {:a 1, :b 2}

#### Nested Hydration

  You can do recursive hydration by listing keys inside a vector:

    (hydrate {...} [:a :b])
      -> {:a {:b 1}}

  The first key in a vector will be hydrated normally, and any subsequent keys
  will be hydrated *inside* the corresponding values for that key.

    (hydrate {...}
             [:a [:b :c] :e])
      -> {:a {:b {:c 1} :e 2}}"
  [results k & ks]
  (when results
    (if (sequential? results)
      (if (empty? results)
        results
        (apply hydrate-forms results k ks))
      (first (apply hydrate-forms [results] k ks)))))
