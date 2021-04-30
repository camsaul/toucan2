(ns bluejdbc.hydrate
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti can-hydrate-with-strategy?*
  {:arglists '([connectable tableable strategy k])}
  u/dispatch-on-first-four-args)

(m/defmulti hydrate-with-strategy*
  {:arglists '([connectable tableable strategy k rows])}
  (fn [_ _ strategy _ _]
    strategy))

;;;                                  Automagic Batched Hydration (via :table-keys)
;;; ==================================================================================================================

(m/defmulti automagic-hydration-key-table*
  {:arglists '([connectable k])}
  u/dispatch-on-first-two-args)

(m/defmethod automagic-hydration-key-table* :default
  [_ _]
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

(m/defmethod can-hydrate-with-strategy?* [:default :default ::automagic-batched :default]
  [connectable tableable strategy k]
  (boolean (automagic-hydration-key-table* connectable k)))

(m/defmethod hydrate-with-strategy* ::automagic-batched
  [connectable tableable strategy dest-key results]
  (let [table       (automagic-hydration-key-table* connectable dest-key)
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


;;;                         Method-Based Batched Hydration (using impls of `batched-hydrate`*)
;;; ==================================================================================================================

(m/defmulti batched-hydrate*
  {:arglists '([connectable tableable k rows])}
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
  {:arglists '([connectable tableable k row])}
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
    (do
      (log/tracef "Hydrating %s with strategy %s" k strategy)
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


  #### Automagic Batched Hydration (via `automagic-hydration-key-table*`)

  `hydrate` attempts to do a *batched hydration* where possible.
  If the key being hydrated is defined as one of some table's `automagic-hydration-key-table*`,
  `hydrate` will do a batched `db/select` if a corresponding key ending with `_id`
  is found in the objects being batch hydrated.

    (hydrate [{:user_id 100}, {:user_id 101}] :user)

  Since `:user` is a hydration key for `User`, a single `db/select` will used to
  fetch `Users`:

    (db/select User :id [:in #{100 101}])

  The corresponding `Users` are then added under the key `:user`.


  #### Function-Based Batched Hydration (via functions marked ^:batched-hydrate*)

  If the key can't be hydrated auto-magically with the appropriate `:automagic-hydration-key-table*`,
  `hydrate` will look for a function tagged with `:batched-hydrate`* in its metadata, and
  use that instead. If a matching function is found, it is called with a collection of objects,
  e.g.

    (defn with-fields
      \"Efficiently add `:fields` to a collection of `tables`.\"
      {:batched-hydrate* :fields}
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

  As with `:batched-hydrate`* functions, by default, the function will be used to hydrate keys that
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
        (let [first-row (first results)]
          (apply hydrate-forms (instance/connectable first-row) (instance/table first-row) results k ks)))
      (first (apply hydrate-forms (instance/connectable results) (instance/table results) [results] k ks)))))
