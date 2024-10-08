(ns toucan2.select
  "Implementation of [[select]] and variations.

  The args spec used by [[select]] lives in [[toucan2.query]], specifically `:toucan2.query/default-args`.

  Code for building Honey SQL for a SELECT lives in [[toucan2.honeysql2]].

  ### Functions that return primary keys

  Functions that return primary keys such as [[select-pks-set]] determine which primary keys to return by
  calling [[toucan2.model/select-pks-fn]], which is based on the model's implementation
  of [[toucan2.model/primary-keys]]. Models with just a single primary key column will return primary keys 'unwrapped',
  i.e., the values of that column will be returned directly. Models with compound primary keys (i.e., primary keys
  consisting of more than one column) will be returned in vectors as if by calling `juxt`.

  ```clj
  ;; A model with a one-column primary key, :id
  (t2/select-pks-vec :models/venues :category \"bar\")
  ;; => [1 2]

  ;; A model with a compound primary key, [:id :name]
  (t2/select-pks-vec :models/venues.compound-key :category \"bar\")
  ;; => [[1 \"Tempest\"] [2 \"Ho's Tavern\"]]
  ```"
  (:refer-clojure :exclude [count])
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]
   [toucan2.types :as types]))

(comment s/keep-me
         types/keep-me)

(defn reducible-select
  "Like [[select]], but returns an `IReduceInit`."
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/select.instances unparsed-args))

(defn select
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/select.instances unparsed-args))

(defn select-one
  "Like [[select]], but only fetches a single row, and returns only that row."
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [& unparsed-args]
  (let [query-type :toucan.query-type/select.instances
        rf         (pipeline/default-rf query-type)
        xform      (pipeline/first-result-xform-fn query-type)]
    (pipeline/transduce-unparsed (xform rf) query-type unparsed-args)))

(defn select-fn-reducible
  "Like [[reducible-select]], but returns a reducible sequence of results of `(f row)`."
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f & unparsed-args]
  (eduction
   (map f)
   (pipeline/reducible-unparsed :toucan.query-type/select.instances.fns unparsed-args)))

(defn select-fn-set
  "Like [[select]], but returns a *set* of values of `(f instance)` for the results. Returns `nil` if the set is empty.

  ```clj
  (t2/select-fn-set (comp str/upper-case :category) :models/venues :category \"bar\")
  ;; =>
  #{\"BAR\"}
  ```"
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f & unparsed-args]
  (let [f     (comp realize/realize f)
        rf    (pipeline/conj-with-init! #{})
        xform (map f)]
    (not-empty (pipeline/transduce-unparsed (xform rf) :toucan.query-type/select.instances.fns unparsed-args))))

(defn select-fn-vec
  "Like [[select]], but returns a *vector* of values of `(f instance)` for the results. Returns `nil` if the vector is
  empty.

  ```clj
  (t2/select-fn-vec (comp str/upper-case :category) :models/venues :category \"bar\")
  ;; =>
  [\"BAR\" \"BAR\"]
  ```

  NOTE: If your query does not specify an `:order-by` clause (or equivalent), the results are like indeterminate. Keep
  this in mind!"
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f & unparsed-args]
  (let [f     (comp realize/realize f)
        rf    (pipeline/conj-with-init! [])
        xform (map f)]
    (not-empty (pipeline/transduce-unparsed (xform rf) :toucan.query-type/select.instances.fns unparsed-args))))

(defn select-one-fn
  "Like [[select-one]], but applies `f` to the result.

  ```clj
  (t2/select-one-fn :id :models/people :name \"Cam\")
  ;; => 1
  ```"
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f & unparsed-args]
  (let [query-type :toucan.query-type/select.instances.fns
        f          (comp realize/realize f)
        rf         (pipeline/with-init conj [])
        xform      (comp (map f)
                         (pipeline/first-result-xform-fn query-type))]
    (pipeline/transduce-unparsed (xform rf) query-type unparsed-args)))

(defn select-pks-reducible
  "Returns a reducible sequence of all primary keys

  ```clj
  (into [] (t2/select-pks-reducible :models/venues :category \"bar\"))
  ;; => [1 2]
  ```"
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [modelable & unparsed-args]
  (apply select-fn-reducible (model/select-pks-fn modelable) modelable unparsed-args))

(defn select-pks-set
  "Returns a *set* of all primary keys (as determined by [[toucan2.model/primary-keys]]
  and [[toucan2.model/select-pks-fn]]) of instances matching the query. Models with just a single primary key columns
  will be 'unwrapped' (i.e., the values of that column will be returned); models with compound primary keys (i.e., more
  than one column) will be returned in vectors as if by calling `juxt`.

  ```clj
  (t2/select-pks-set :models/venues :category \"bar\")
  ;; => #{1 2}
  ```"
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [modelable & unparsed-args]
  (apply select-fn-set (model/select-pks-fn modelable) modelable unparsed-args))

(defn select-pks-vec
  "Returns a *vector* of all primary keys (as determined by [[toucan2.model/primary-keys]]
  and [[toucan2.model/select-pks-fn]]) of instances matching the query. Models with just a single primary key columns
  will be 'unwrapped' (i.e., the values of that column will be returned); models with compound primary keys (i.e., more
  than one column) will be returned in vectors as if by calling `juxt`.

  ```clj
  (t2/select-pks-vec :models/venues :category \"bar\")
  ;; => [1 2]
  ```

  NOTE: If your query does not specify an `:order-by` clause (or equivalent), the results are like indeterminate. Keep
  this in mind!"
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [modelable & unparsed-args]
  (apply select-fn-vec (model/select-pks-fn modelable) modelable unparsed-args))

(defn select-one-pk
  "Return the primary key of the first row matching the query. Models with just a single primary key columns will be
  'unwrapped' (i.e., the values of that column will be returned); models with compound primary keys (i.e., more than one
  column) will be returned in vectors as if by calling `juxt`.

  ```clj
  (t2/select-one-pk :models/people :name \"Cam\")
  ;; => 1
  ```"
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [modelable & unparsed-args]
  (apply select-one-fn (model/select-pks-fn modelable) modelable unparsed-args))

(defn select-fn->fn
  "Return a map of `(f1 instance)` -> `(f2 instance)` for instances matching the query.

  ```clj
  (t2/select-fn->fn :id (comp str/upper-case :name) :models/people)
  ;; => {1 \"CAM\", 2 \"SAM\", 3 \"PAM\", 4 \"TAM\"}
  ```"
  {:arglists '([f1 f2 modelable-columns & kv-args? query?]
               [f1 f2 :conn connectable modelable-columns & kv-args? query?])}
  [f1 f2 & unparsed-args]
  (let [f1    (comp realize/realize f1)
        f2    (comp realize/realize f2)
        rf    (pipeline/conj-with-init! {})
        xform (map (juxt f1 f2))]
    (pipeline/transduce-unparsed (xform rf) :toucan.query-type/select.instances unparsed-args)))

(defn select-fn->pk
  "The inverse of [[select-pk->fn]]. Return a map of `(f instance)` -> *primary key* for instances matching the query.

  ```clj
  (t2/select-fn->pk (comp str/upper-case :name) :models/people)
  ;; => {\"CAM\" 1, \"SAM\" 2, \"PAM\" 3, \"TAM\" 4}
  ```"
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (model/select-pks-fn modelable)]
    (apply select-fn->fn f pks-fn modelable args)))

(defn select-pk->fn
  "The inverse of [[select-fn->pk]]. Return a map of *primary key* -> `(f instance)` for instances matching the query.

  ```clj
  (t2/select-pk->fn (comp str/upper-case :name) :models/people)
  ;; => {1 \"CAM\", 2 \"SAM\", 3 \"PAM\", 4 \"TAM\"}
  ```"
  {:arglists '([f modelable-columns & kv-args? query?]
               [f :conn connectable modelable-columns & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (model/select-pks-fn modelable)]
    (apply select-fn->fn pks-fn f modelable args)))

(defn- count-rf []
  (let [logged-warning? (atom false)
        log-warning     (fn []
                          (when-not @logged-warning?
                            (log/warnf "Warning: inefficient count query. See documentation for toucan2.select/count.")
                            (reset! logged-warning? true)))]
    (fn count-rf*
      ([] 0)
      ([acc] acc)
      ([acc row]
       (if (:count row)
         (+ acc (:count row))
         (do (log-warning)
             (inc acc)))))))

(defn count
  "Like [[select]], but returns the number of rows that match in an efficient way.

  ### Implementation note:

  The default Honey SQL 2 map query compilation backend builds an efficient

  ```sql
  SELECT count(*) AS \"count\" FROM ...
  ```

  query. Custom query compilation backends should do the equivalent by implementing [[toucan2.pipeline/build]] for the
  query type `:toucan.query-type/select.count` and build a query that returns the key `:count`, If an efficient
  implementation does not exist, this will fall back to simply counting all matching rows."
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed (count-rf) :toucan.query-type/select.count unparsed-args))

(defn- exists?-rf
  ([] false)
  ([acc] acc)
  ([_acc row]
   (if (contains? row :exists)
     (let [exists (:exists row)
           result (if (integer? exists)
                    (pos? exists)
                    (boolean exists))]
       (if (true? result)
         (reduced true)
         false))
     (do
       (log/warnf "Warning: inefficient exists? query. See documentation for toucan2.select/exists?.")
       (reduced true)))))

(defn exists?
  "Like [[select]], but returns whether or not *any* rows match in an efficient way.

  ### Implementation note:

  The default Honey SQL 2 map query compilation backend builds an efficient

  ```sql
  SELECT exists(SELECT 1 FROM ... WHERE ...) AS exists
  ```"
  {:arglists '([modelable-columns & kv-args? query?]
               [:conn connectable modelable-columns & kv-args? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed exists?-rf :toucan.query-type/select.exists unparsed-args))
