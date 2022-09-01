(ns toucan2.query
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.map-backend :as map]
   [toucan2.model :as model]
   [toucan2.util :as u]))

;;;; [[parse-args]]

(s/def ::default-args.modelable
  (s/or
   :modelable         (complement sequential?)
   :modelable-columns (s/cat :modelable some? ; can't have a nil model. Or can you?
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
     with [[toucan2.model/resolve-model]].

  * `:queryable` -- something that can be resolved to a query, for example a map or integer or 'named query' keyword.
    The resolved query is ultimately combined with other parsed args and built into something like a Honey SQL map, then
    compiled to something like SQL.

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


;;;; Part of the default [[pipeline/transduce-build]] for maps: applying key-value args

(m/defmulti apply-kv-arg
  {:arglists '([model₁ query₂ k₃ v])}
  u/dispatch-on-first-three-args)

;;; not 100% sure we need this method since the query should already have `:type` metadata, but it's nice to have it
;;; anyway so we can play around with this stuff from the REPL
(m/defmethod apply-kv-arg [#_model :default #_query clojure.lang.IPersistentMap #_k :default]
  [model query k v]
  (apply-kv-arg model (vary-meta query assoc :type (map/backend)) k v))

;;;; See [[toucan2.map-backend.honeysql2]] for the Honey SQL-specific impl for this.

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

(defn- apply-non-composite-toucan-pk [model m pk-column v]
  ;; unwrap the value if we got something like `:toucan/pk [1]`
  (let [v (if (and (sequential? v)
                   (not (keyword? (first v)))
                   (= (count v) 1))
            (first v)
            v)]
    (apply-kv-arg model m pk-column v)))

(defn- apply-composite-toucan-pk [model m pk-columns v]
  (reduce
   (fn [m {:keys [col v]}]
     (apply-kv-arg model m col v))
   m
   (toucan-pk-composite-values pk-columns v)))

;;; `:around` so we can intercept the normal handler. This "unpacks" the PK and ultimately uses the normal calls to
;;; [[apply-kv-arg]]
(m/defmethod apply-kv-arg :around [#_model :default #_query :default #_k :toucan/pk]
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
