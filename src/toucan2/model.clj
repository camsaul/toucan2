(ns toucan2.model
  "Methods related to resolving Toucan 2 models, appropriate table names to use when building queries for them, and
  namespaces to use for columns in query results."
  (:refer-clojure :exclude [namespace])
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment s/keep-me
         types/keep-me)

(m/defmulti resolve-model
  "Resolve a *modelable* to an actual Toucan *model* (usually a keyword). A modelable is anything that can be resolved to
  a model via this method. You can implement this method to define special model resolution behavior, for example
  `toucan2-toucan1` defines a method for `clojure.lang.Symbol` that does namespace resolution to return an appropriate
  model keyword.

  You can also implement this method to do define behaviors when a model is used, for example making sure some namespace
  with method implementation for the model is loaded, logging some information, etc."
  {:arglists '([modelable₁]), :defmethod-arities #{1}}
  u/dispatch-on-first-arg)

(m/defmethod resolve-model :default
  "Default implementation. Return `modelable` as is, i.e., there is nothing to resolve, and we can use it directly as a
  model."
  [modelable]
  modelable)

(m/defmethod resolve-model :around :default
  "Log model resolution as it happens for debugging purposes."
  [modelable]
  (let [model (next-method modelable)]
    (log/debugf "Resolved modelable %s => model %s" modelable model)
    model))

(m/defmulti default-connectable
  "The default connectable that should be used when executing queries for `model` if
  no [[toucan2.connection/*current-connectable*]] is currently bound. By default, this just returns the global default
  connectable, `:default`, but you can tell Toucan to use a different default connectable for a model by implementing
  this method."
  {:arglists '([model₁]), :defmethod-arities #{1}}
  u/dispatch-on-first-arg)

(m/defmethod default-connectable :default
  "Return `nil`, so we can fall thru to something else (presumably `:default` anyway)?"
  [_model]
  nil)

(m/defmulti table-name
  "Return the actual underlying table name that should be used to query a `model`.

  By default for things that implement `name`, the table name is just `(keyword (name x))`.

  ```clj
  (t2/table-name :models/user)
  ;; =>
  :user
  ```

  You can write your own implementations for this for models whose table names do not match their `name`.

  This is guaranteed to return a keyword, so it can easily be used directly in Honey SQL queries and the like; if you
  return something else, the default `:after` method will convert it to a keyword for you."
  {:arglists            '([model₁])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod table-name :default
  "Fallback implementation. Redirects keywords to the implementation for `clojure.lang.Named` (use the `name` of the
  keyword). For everything else, throws an error, since we don't know how to get a table name from it."
  [model]
  (if (instance? clojure.lang.Named model)
    ((m/effective-method table-name clojure.lang.Named) model)
    (throw (ex-info (format "Invalid model %s: don't know how to get its table name." (pr-str model))
                    {:model model}))))

(m/defmethod table-name :after :default
  "Always return table names as keywords. This will facilitate using them directly inside Honey SQL, e.g.

    {:select [:*], :from [(t2/table-name MyModel)]}"
  [a-table-name]
  (keyword a-table-name))

(m/defmethod table-name clojure.lang.Named
  "Default implementation for anything that is a `clojure.lang.Named`, such as a keywords or symbols. Use the `name` as
  the table name.

  ```clj
  (t2/table-name :models/user) => :user
  ```"
  [model]
  (name model))

(m/defmethod table-name String
  "Implementation for strings. Use the string name as-is."
  [table]
  table)

(m/defmulti primary-keys
  "Return a sequence of the primary key columns names, as keywords, for a model. The default primary keys for a model are
  `[:id]`; implement this method if your model has different primary keys.

  ```clj
  ;; tell Toucan that :model/bird has a composite primary key consisting of the columns :id and :name
  (m/defmethod primary-keys :model/bird
    [_model]
    [:id :name])
  ```

  If an implementation returns a single keyword, the default `:around` method will automatically wrap it in a vector. It
  also validates that the ultimate result is a sequence of keywords, so it is safe to assume that calls to this will
  always return a sequence of keywords."
  {:arglists            '([model₁])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod primary-keys :around :default
  "If the PK comes back unwrapped, wrap it -- make sure results are always returned as a vector of keywords. Throw an
  error if results are in the incorrect format."
  [model]
  (let [pk-or-pks (next-method model)
        pks       (if (sequential? pk-or-pks)
                    pk-or-pks
                    [pk-or-pks])]
    (when-not (every? keyword? pks)
      (throw (ex-info (format "Bad %s for model %s: should return keyword or sequence of keywords, got %s"
                              `primary-keys
                              (pr-str model)
                              (pr-str pk-or-pks))
                      {:model model, :result pk-or-pks})))
    pks))

(defn primary-key-values-map
  "Return a map of primary key values for a Toucan 2 `instance`."
  ([instance]
   (primary-key-values-map (protocols/model instance) instance))
  ([model m]
   ;; clojure.core/select-keys is quite inefficient on Toucan2 Instances; using a custom implementation.
   (let [ks (set (primary-keys model))]
     (reduce-kv (fn [acc k v]
                  (cond-> acc
                    (contains? ks k) (assoc k v)))
                {} m))))

;;; TODO -- consider renaming this to something better. What?
(defn select-pks-fn
  "Return a function to get the value(s) of the primary key(s) from a row, as a single value or vector of values. Used
  by [[toucan2.select/select-pks-reducible]] and thus
  by [[toucan2.select/select-pks-set]], [[toucan2.select/select-pks-vec]], etc.

  The primary keys are determined by [[primary-keys]]. By default this is simply the keyword `:id`."
  [modelable]
  (let [model   (resolve-model modelable)
        pk-keys (primary-keys model)]
    (if (= (count pk-keys) 1)
      (first pk-keys)
      (apply juxt pk-keys))))

(m/defmulti model->namespace
  "Return a map of namespaces to use when fetching results with this model.

  ```clj
  (m/defmethod model->namespace ::my-model
    [_model]
    {::my-model      \"x\"
     ::another-model \"y\"})
  ```"
  {:arglists            '([model₁])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.model)}
  u/dispatch-on-first-arg)

(m/defmethod model->namespace :default
  "By default, don't namespace column names when fetching rows."
  [_model]
  nil)

(m/defmethod model->namespace :after :default
  "Validate the results."
  [namespace-map]
  (when (some? namespace-map)
    (assert (map? namespace-map)
            (format "model->namespace should return a map. Got: ^%s %s"
                    (some-> namespace-map class .getCanonicalName)
                    (pr-str namespace-map))))
  namespace-map)

(defn table-name->namespace
  "Take the [[model->namespace]] map for a model and return a map of string table name -> namespace. This is used to
  determine how to prefix columns in results based on their table name;
  see [[toucan2.jdbc.result-set/instance-builder-fn]] for an example of this usage."
  [model]
  (not-empty
   (into {}
         (comp (filter (fn [[model _a-namespace]]
                         (not= (m/effective-primary-method table-name model)
                               (m/default-effective-method table-name))))
               (map (fn [[model a-namespace]]
                      [(name (table-name model)) a-namespace])))
         (model->namespace model))))

(defn namespace
  "Get the namespace that should be used to prefix keys associated with a `model` in query results. This is taken from the
  model's implementation of [[model->namespace]]."
  [model]
  (some
   (fn [[a-model a-namespace]]
     (when (isa? model a-model)
       a-namespace))
   (model->namespace model)))

(m/defmethod primary-keys :default
  "By default the primary key for a model is the column `:id`; or `:some-namespace/id` if the model defines a namespace
  for itself with [[model->namespace]]."
  [model]
  (if-let [model-namespace (namespace model)]
    [(keyword (name model-namespace) "id")]
    [:id]))
