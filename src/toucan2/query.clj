(ns toucan2.query
  "Query compilation pipeline is something like this:

  ```
  Call something like [[toucan2.select/select]] with `modelable` and `unparsed-args`
  ↓
  `modelable` is resolved to `model` with [[toucan2.model/with-model]]
  ↓
  [[parse-args]] is used to parse args into a `parsed-args` map
  ↓
  The `:queryable` key in parsed args is resolved
  ↓
  [[build]] takes the resolved query and combines the other parsed args into it to build a compilable query. The default
  backend builds a Honey SQL 2 query map
  ↓
  some sort of reducible is returned.

  <reducing the reducible>
  ↓
  [[toucan2.compile/with-compiled-query]] compiles the built query (e.g. Honey SQL 2) into something that can be
  executed natively (e.g. a `sql-args` vector)
  ↓
  [[toucan2.connection/with-connection]] is used to get a connection from the current connectable, or the default
  connectable for `model`, or the global default connectable
  ↓
  Compiled query is executed with connection
  ```"
  (:require
   [better-cond.core :as b]
   [clojure.spec.alpha :as s]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.util :as u]))

;;;; [[parse-args]]

(s/def ::default-args.modelable
  (s/or
   :modelable         (complement sequential?)
   :modelable-columns (s/cat :modelable some? ; can't have a nil model.
                             :columns   (s/* keyword?))))

;;; TODO -- can we use [[s/every-kv]] for this stuff?
(s/def ::default-args.kv-args
  (s/* (s/cat
        :k keyword?
        :v any?)))

(s/def ::default-args.kv-args.non-empty
  (s/+ (s/cat
        :k keyword?
        :v any?)))

(s/def ::default-args.queryable
  (s/? any?))

(s/def ::default-args
  (s/cat
   :modelable ::default-args.modelable
   :kv-args   ::default-args.kv-args
   :queryable ::default-args.queryable))

(s/def :toucan2.query.parsed-args/modelable
  some?)

(s/def :toucan2.query.parsed-args/kv-args
  (some-fn nil? map?))

(s/def :toucan2.query.parsed-args/columns
  (some-fn nil? sequential?))

(s/def ::parsed-args
  (s/keys :req-un [:toucan2.query.parsed-args/modelable]
          :opt-un [:toucan2.query.parsed-args/kv-args
                   :toucan2.query.parsed-args/columns]))

(defn validate-parsed-args [parsed-args]
  (u/try-with-error-context ["validate parsed args" {::parsed-args parsed-args}]
    (let [result (s/conform ::parsed-args parsed-args)]
      (when (s/invalid? result)
        (throw (ex-info (format "Invalid parsed args: %s" (s/explain-str ::parsed-args parsed-args))
                        (s/explain-data ::parsed-args parsed-args)))))))

(defn parse-args
  "`parse-args` takes a sequence of unparsed args passed to something like [[toucan2.select/select]] and parses them into
  a parsed args map. The default implementation uses [[clojure.spec.alpha]] to parse the args according to `args-spec`.

  These keys are commonly returned by several of the different implementations `parse-args`, and other tooling is
  build to leverage them:

  * `:modelable` -- usually the first of the `unparsed-args`, this is the thing that should get resolved to a model
     with [[toucan2.model/with-model]].

  * `:queryable` -- something that can be resolved to a query, for example a map or integer or 'named query' keyword.
    The resolved query is ultimately combined with other parsed args and built into something like a Honey SQL map
    with [[build]], then compiled with [[toucan2.compile/with-compiled-query]].

  * `:kv-args` -- map of key-value pairs. When [[build]] builds a query, it calls [[apply-kv-arg]] for each of the
    key-value pairs. The default behavior is to append a Honey SQL `:where` clause based on the pair; but you can
    customize the behavior for specific keywords to do other things -- `:toucan/pk` is one such example.

  * `:columns` -- for things that return instances, `:columns` is a sequence of columns to return. These are commonly
    specified by wrapping the modelable in a `[modelable & columns]` vector."
  ([query-type unparsed-args]
   (parse-args query-type ::default-args unparsed-args))

  ([query-type spec unparsed-args]
   (u/try-with-error-context ["parse args" {::query-type query-type, ::unparsed-args unparsed-args}]
     (u/with-debug-result (list `parse-args query-type unparsed-args)
       (let [parsed (s/conform spec unparsed-args)]
         (when (s/invalid? parsed)
           (throw (ex-info (format "Don't know how to interpret %s args: %s"
                                   (u/safe-pr-str query-type)
                                   (s/explain-str spec unparsed-args))
                           (s/explain-data spec unparsed-args))))
         (doto (cond-> parsed
                 (:modelable parsed)                 (merge (let [[modelable-type x] (:modelable parsed)]
                                                              (case modelable-type
                                                                :modelable         {:modelable x}
                                                                :modelable-columns x)))
                 (not (contains? parsed :queryable)) (assoc :queryable {})
                 (seq (:kv-args parsed))             (update :kv-args (fn [kv-args]
                                                                        (into {} (map (juxt :k :v)) kv-args))))
           validate-parsed-args))))))


;;;; Default [[pipeline/transduce-resolved-query]] impl for maps; applying key-value args.

(m/defmulti apply-kv-arg
  {:arglists '([model₁ query₂ k₃ v])}
  u/dispatch-on-first-three-args)

(defn- fn-condition->honeysql-where-clause
  [k [f & args]]
  {:pre [(keyword? f) (seq args)]}
  (into [f k] args))

(defn condition->honeysql-where-clause
  "Something sequential like `:id [:> 5]` becomes `[:> :id 5]`. Other stuff like `:id 5` just becomes `[:= :id 5]`."
  [k v]
  ;; don't think there's any situtation where `nil` on the LHS is on purpose and not a bug.
  {:pre [(some? k)]}
  (if (sequential? v)
    (fn-condition->honeysql-where-clause k v)
    [:= k v]))

(m/defmethod apply-kv-arg [:default clojure.lang.IPersistentMap :default]
  [_model honeysql k v]
  (u/with-debug-result ["apply kv-arg %s %s" k v]
    (update honeysql :where (fn [existing-where]
                              (:where (hsql.helpers/where existing-where
                                                          (condition->honeysql-where-clause k v)))))))

(comment
  ;; with a composite PK like
  [:id :name]
  ;; we need to be able to handle either
  [:in [["BevMo" 4] ["BevLess" 5]]]
  ;; or
  [:between ["BevMo" 4] ["BevLess" 5]])

(defn- toucan-pk-composite-values** [pk-columns tuple]
  {:pre [(= (count pk-columns) (count tuple))]}
  (map-indexed (fn [i col]
                 {:col col, :v (nth tuple i)})
               pk-columns))

(defn- toucan-pk-nested-composite-values [pk-columns tuples]
  (->> (mapcat (fn [tuple]
                 (toucan-pk-composite-values** pk-columns tuple))
               tuples)
       (group-by :col)
       (map (fn [[col ms]]
              {:col col, :v (mapv :v ms)}))))

(defn- toucan-pk-composite-values* [pk-columns tuple]
  (if (some sequential? tuple)
    (toucan-pk-nested-composite-values pk-columns tuple)
    (toucan-pk-composite-values** pk-columns tuple)))

(defn- toucan-pk-fn-values [pk-columns fn-name tuples]
  (->> (mapcat (fn [tuple]
                 (toucan-pk-composite-values* pk-columns tuple))
               tuples)
       (group-by :col)
       (map (fn [[col ms]]
              {:col col, :v (into [fn-name] (map :v) ms)}))))

(defn- toucan-pk-composite-values [pk-columns tuple]
  {:pre [(sequential? tuple)], :post [(sequential? %) (every? map? %) (every? :col %)]}
  (if (keyword? (first tuple))
    (toucan-pk-fn-values pk-columns (first tuple) (rest tuple))
    (toucan-pk-composite-values* pk-columns tuple)))

(defn- apply-non-composite-toucan-pk [model honeysql pk-column v]
  ;; unwrap the value if we got something like `:toucan/pk [1]`
  (let [v (if (and (sequential? v)
                   (not (keyword? (first v)))
                   (= (count v) 1))
            (first v)
            v)]
    (apply-kv-arg model honeysql pk-column v)))

(defn- apply-composite-toucan-pk [model honeysql pk-columns v]
  (reduce
   (fn [honeysql {:keys [col v]}]
     (apply-kv-arg model honeysql col v))
   honeysql
   (toucan-pk-composite-values pk-columns v)))

(m/defmethod apply-kv-arg [:default clojure.lang.IPersistentMap :toucan/pk]
  [model honeysql _k v]
  ;; `fn-name` here would be if you passed something like `:toucan/pk [:in 1 2]` -- the fn name would be `:in` -- and we
  ;; pass that to [[condition->honeysql-where-clause]]
  (let [pk-columns (model/primary-keys model)]
    (u/with-debug-result ["apply :toucan/pk %s for primary keys" v]
      (if (= (count pk-columns) 1)
        (apply-non-composite-toucan-pk model honeysql (first pk-columns) v)
        (apply-composite-toucan-pk model honeysql pk-columns v)))))

(defn apply-kv-args [model query kv-args]
  (reduce
   (fn [query [k v]]
     (apply-kv-arg model query k v))
   query
   kv-args))

(defn honeysql-table-and-alias
  "Build an Honey SQL `[table]` or `[table alias]` (if the model has a [[toucan2.model/namespace]] form) for `model` for
  use in something like a `:select` clause."
  [model]
  (b/cond
    :let [table-id (keyword (model/table-name model))
          alias-id (model/namespace model)
          alias-id (when alias-id
                     (keyword alias-id))]
    alias-id
    [table-id alias-id]

    :else
    [table-id]))

;; (defn- format-identifier [_ parts]
;;   [(str/join \. (map hsql/format-entity parts))])

;; (hsql/register-fn! ::identifier #'format-identifier)
