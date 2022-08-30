(ns toucan2.query
  "Query compilation pipeline is something like this:

  ```
  Call something like [[toucan2.select/select]] with `modelable` and `unparsed-args`
  ↓
  `modelable` is resolved to `model` with [[toucan2.model/with-model]]
  ↓
  [[parse-args]] is used to parse args into a `parsed-args` map
  ↓
  The `:queryable` key in parsed args is resolved and replaced with a `:query` by [[with-resolved-query]]
  ↓
  [[build]] takes the `:query` in the parsed args and combines the other parsed args into it to build a
  compilable query. The default backend builds a Honey SQL 2 query map
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
   [clojure.spec.alpha :as s]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

;;;; [[parse-args]]

(m/defmulti args-spec
  "[[clojure.spec.alpha]] spec that should be used to parse unparsed args for `query-type` by the default implementation
  of [[parse-args]]."
  {:arglists '([query-type])}
  u/dispatch-on-first-arg)

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

(m/defmethod args-spec :default
  [_query-type]
  ::default-args)

(m/defmulti parse-args
  "`parse-args` takes a sequence of unparsed args passed to something like [[toucan2.select/select]] and parses them into
  a parsed args map. The default implementation uses [[clojure.spec.alpha]] to parse the args according to
  the [[args-spec]] for `query-type`.

  These keys are commonly returned by several of the different implementations `parse-args`, and other tooling is
  build to leverage them:

  * `:modelable` -- usually the first of the `unparsed-args`, this is the thing that should get resolved to a model
     with [[toucan2.model/with-model]].

  * `:queryable` -- something that can be resolved to a query with [[with-resolved-query]], for example a map or integer or
    'named query' keyword. The resolved query is ultimately combined with other parsed args and built into something like
    a Honey SQL map with [[build]], then compiled with [[toucan2.compile/with-compiled-query]].

  * `:kv-args` -- map of key-value pairs. When [[build]] builds a query, it calls [[apply-kv-arg]] for each of the
    key-value pairs. The default behavior is to append a Honey SQL `:where` clause based on the pair; but you can
    customize the behavior for specific keywords to do other things -- `:toucan/pk` is one such example.

  * `:columns` -- for things that return instances, `:columns` is a sequence of columns to return. These are commonly
    specified by wrapping the modelable in a `[modelable & columns]` vector."
  {:arglists '([query-type₁ unparsed-args])}
  u/dispatch-on-first-arg)

;;;; the stuff below validates parsed args in the `:default` `:around` method.

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

(m/defmethod parse-args :around :default
  [query-type unparsed-args]
  (u/try-with-error-context ["parse args" {::query-type query-type, ::unparsed-args unparsed-args}]
    (doto (u/with-debug-result (list `parse-args query-type unparsed-args)
            (next-method query-type unparsed-args))
      validate-parsed-args)))

;;;; default method

(m/defmethod parse-args :default
  [query-type unparsed-args]
  (let [spec   (args-spec query-type)
        parsed (s/conform spec unparsed-args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret %s args: %s"
                              (u/safe-pr-str query-type)
                              (s/explain-str spec unparsed-args))
                      (s/explain-data spec unparsed-args))))
    (cond-> parsed
      (:modelable parsed)                 (merge (let [[modelable-type x] (:modelable parsed)]
                                                   (case modelable-type
                                                     :modelable         {:modelable x}
                                                     :modelable-columns x)))
      (not (contains? parsed :queryable)) (assoc :queryable {})
      (seq (:kv-args parsed))             (update :kv-args (fn [kv-args]
                                                             (into {} (map (juxt :k :v)) kv-args))))))

;;;; [[do-with-resolved-query]] and [[with-resolved-query]]

(m/defmulti do-with-resolved-query
  "Impls should resolve `queryable` to an *unbuilt* query that can be built by [[build]] and call

  ```clj
  (f query)
  ```

  Example:

  ```clj
  ;; define a custom query ::my-count that you can then use with select and the like
  (m/defmethod do-with-resolved-query [:default ::my-count]
    [model _queryable f]
    (do-with-resolved-query model {:select [:%count.*], :from [(keyword (model/table-name model))]} f))

  (select :model/user ::my-count)
  ```

  Dispatches off of `[modelable queryable]`."
  {:arglists '([model₁ queryable₂ f])}
  u/dispatch-on-first-two-args)

;; define a custom query `::my-count`
(m/defmethod do-with-resolved-query [:default ::my-count]
  [model _queryable f]
  (do-with-resolved-query model {:select [:%count.*], :from [(keyword (model/table-name model))]} f))

(defmacro with-resolved-query
  "Resolve a `queryable` to an *unbuilt* query and bind it to `query-binding`. After resolving the query the next step is
  to build it into a compilable query using [[build]].

  ``clj
  (with-resolved-query [resolved-query [:model/user :some-named-query]]
    (build model :toucan2.select/select (assoc parsed-args :query resolved-query)))
  ```"
  {:style/indent :defn}
  [[query-binding [model queryable]] & body]
  `(do-with-resolved-query ~model ~queryable (^:once fn* [query#]
                                              ;; support destructing the query.
                                              (let [~query-binding query#]
                                                ~@body))))

(s/fdef with-resolved-query
  :args (s/cat :bindings (s/spec (s/cat :query               :clojure.core.specs.alpha/binding-form
                                        :modelable+queryable (s/spec (s/cat :model     any? ; I guess model can be `nil` here
                                                                            :queryable any?))))
               :body     (s/+ any?))
  :ret any?)

(m/defmethod do-with-resolved-query :default
  [_model queryable f]
  (let [f* (^:once fn* [query]
            (assert (not (:queryable query))
                    "Don't pass parsed-args to do-with-resolved-query. Pass just the queryable")
            (when (and u/*debug*
                       (not= query queryable))
              (u/with-debug-result ["%s: resolved queryable %s" `do-with-resolved-query queryable]
                query))
            (f query))]
    (f* queryable)))

(m/defmethod do-with-resolved-query :around :default
  [model queryable f]
  (let [f* (^:once fn* [query]
            (u/try-with-error-context (when (not= queryable query)
                                        ["with resolved query" {::model model, ::queryable queryable, ::resolved-query query}])
              (f query)))]
    (next-method model queryable f*)))

;;;; [[build]]

;;; TODO -- it's a little weird that literally every other multimethod in the query execution/compilation pipeline is
;;; `do-with-` while this one isn't. Should this be `do-with-built-query` or something like that?
;;;
;;; TODO -- I'm 99% sure that `query` should be a separate arg from the rest of the `parsed-args`.
(m/defmulti build
  "`build` takes the parsed args returned by [[parse]] and builds them into a query that can be compiled
  by [[toucan2.compile/with-compiled-query]]. For the default implementations, `build` takes the parsed arguments and
  builds a Honey SQL 2 map."
  {:arglists '([query-type₁ model₂ {:keys [query₃], :as parsed-args}])}
  (fn [query-type model parsed-args]
    (mapv protocols/dispatch-value [query-type
                                    model
                                    (:query parsed-args)])))

(m/defmethod build :around :default
  [query-type model parsed-args]
  (assert (and (map? parsed-args)
               (contains? parsed-args :query))
          (format "%s expects map parsed-args with :query key, got %s." `build (u/safe-pr-str parsed-args)))
  (u/try-with-error-context ["build query" {::query-type  query-type
                                            ::model       model
                                            ::parsed-args parsed-args}]
    (u/with-debug-result ["%s %s %s with parsed args %s" `build query-type model parsed-args]
      (next-method query-type model parsed-args))))

(m/defmethod build :default
  [query-type model parsed-args]
  (throw (ex-info (format "Don't know how to build a query from parsed args %s. Do you need to implement %s for %s?"
                          (u/safe-pr-str parsed-args)
                          `build
                          (u/safe-pr-str (m/dispatch-value build query-type model parsed-args)))
                  {:query-type     query-type
                   :model          model
                   :parsed-args    parsed-args
                   :method         #'build
                   :dispatch-value (m/dispatch-value build query-type model parsed-args)})))

;;; Something like (select my-model nil) should basically mean SELECT * FROM my_model WHERE id IS NULL
(m/defmethod build [#_query-type :default
                    #_model      :default
                    #_query      nil]
  [query-type model parsed-args]
  (let [parsed-args (cond-> (assoc parsed-args :query {})
                      ;; if `:query` is present but equal to `nil`, treat that as if the pk value IS NULL
                      (contains? parsed-args :query)
                      (update :kv-args assoc :toucan/pk nil))]
    (build query-type model parsed-args)))

(m/defmethod build [#_query-type :default
                    #_model      :default
                    #_query      Integer]
  [query-type model parsed-args]
  (build query-type model (update parsed-args :query long)))

(m/defmethod build [#_query-type :default
                    #_model      :default
                    #_query      Long]
  [query-type model {pk :query, :as parsed-args}]
  (build query-type model (-> parsed-args
                              (assoc :query {})
                              (update :kv-args assoc :toucan/pk pk))))

(m/defmethod build [#_query-type :default
                    #_model      :default
                    #_query      String]
  [query-type model parsed-args]
  (build query-type model (update parsed-args :query (fn [sql]
                                                       [sql]))))

(m/defmethod build [#_query-type :default
                    #_model      :default
                    #_query      clojure.lang.Sequential]
  [query-type model {sql-args :query, :keys [kv-args], :as parsed-args}]
  (when (seq kv-args)
    (throw (ex-info "key-value args are not supported for plain SQL queries."
                    {:query-type     query-type
                     :model          model
                     :parsed-args    parsed-args
                     :method         #'build
                     :dispatch-value (m/dispatch-value build query-type model parsed-args)})))
  sql-args)

(defmacro with-built-query [[built-query-binding [query-type model parsed-args resolved-query]] & body]
  `(let [~built-query-binding (build ~query-type ~model (assoc ~parsed-args :query ~resolved-query))]
     ~@body))

(s/fdef with-built-query
  :args (s/cat :bindings (s/spec (s/cat :lhs :clojure.core.specs.alpha/binding-form
                                        :rhs (s/spec (s/cat :query-type     some?
                                                            :model          any?
                                                            :parsed-args    any?
                                                            :resolved-query any?))))
               :body     (s/+ any?))
  :ret any?)

;;;; Default [[build]] impl for maps; applying key-value args.

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

(m/defmethod build [:default :default clojure.lang.IPersistentMap]
  [_query-type model {:keys [kv-args query], :as _args}]
  (apply-kv-args model query kv-args))
