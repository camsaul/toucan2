(ns toucan2.query
  (:require
   [clojure.spec.alpha :as s]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u]))

;;; Query compilation pipeline is something like this
;;;
;;; Call something like [[toucan2.select/select]] with `modelable` and `unparsed-args`
;;; ↓
;;; `modelable` is resolved to `model` with [[toucan2.model/with-model]]
;;; ↓
;;; [[toucan2.query/parse-args]] is used to parse args into a `parsed-args` map
;;; ↓
;;; The `:queryable` key in parsed args is resolved and replaced with a `:query` by [[toucan2.query/with-query]]
;;; ↓
;;; [[toucan2.query/build]] takes the `:query` in the parsed args and combines the other parsed args into it to build a
;;; compilable query. The default backend builds a Honey SQL 2 query map
;;; ↓
;;; some sort of reducible is returned.
;;;
;;; <reducing the reducible>
;;; ↓
;;; [[toucan2.compile/with-compiled-query]] compiles the built query (e.g. Honey SQL 2) into something that can be
;;; executed natively (e.g. a `sql-args` vector)
;;; ↓
;;; [[toucan2.connection/with-connection]] is used to get a connection from the current connectable, or the default
;;; connectable for `model`, or the global default connectable
;;; ↓
;;; Compiled query is executed with connection

;;;; [[parse-args]]

;;; TODO -- not 100% sure this should dispatch off of model -- seems dangerous and error-prone to let people totally
;;; change up the syntax for something like [[toucan2.select/select]] for one specific model. If you want weird syntax
;;; then write a wrapper function like we do in the `toucan2-toucan1` compatibility layer
(m/defmulti args-spec
  "[[clojure.spec.alpha]] spec that should be used to parse unparsed args for `query-type` by the default implementation
  of [[parse-args]]."
  {:arglists '([query-type model])}
  u/dispatch-on-first-two-args)

(s/def ::default-args
  (s/cat
   :kv-args (s/* (s/cat
                  :k keyword?
                  :v any?))
   :queryable  (s/? any?)))

(m/defmethod args-spec :default
  [_query-type _model]
  ::default-args)

;;; TODO -- as with [[args-spec]] I don't know if having this dispatch off of model as well is a good idea. If it
;;; doesn't dispatch off of model then we can actually include `modelable` itself in the spec and handle
;;; `modelable-columns` as well here instead of separately.
(m/defmulti parse-args
  "`parse-args` takes a sequence of unparsed args passed to something like [[toucan2.select/select]] (excluding the
  `modelable` arg, which has to be resolved to a `model` first in order for this to be able to dispatch), and parses
  them into a parsed args map. The default implementation uses [[clojure.spec.alpha]] to parse the args according to
  the [[args-spec]] for `query-type` and `model`.

  These keys are commonly returned by several of the different implementations `parse-args`, and other tooling is
  build to leverage them:

  * `:queryable` -- something that can be resolved to a query with [[with-query]], for example a map or integer or
    'named query' keyword. The resolved query is ultimately combined with other parsed args and built into something like
    a Honey SQL map with [[build]], then compiled with [[toucan2.compile/with-compiled-query]].

  * `:kv-args` -- map of key-value pairs. When [[build]] builds a query, it calls [[apply-kv-arg]] for each of the
    key-value pairs. The default behavior is to append a Honey SQL `:where` clause based on the pair; but you can
    customize the behavior for specific keywords to do other things -- `:toucan/pk` is one such example.

  * `:columns` -- for things that return instances, `:columns` is a sequence of columns to return. These are commonly
    specified by wrapping the modelable in a `[modelable & columns]` vector."
  {:arglists '([query-type model unparsed-args]
               [query-type model unparsed-args])}
  u/dispatch-on-first-two-args)

(s/def :toucan2.query.parsed-args/kv-args
  (some-fn nil? map?))

(s/def :toucan2.query.parsed-args/columns
  (some-fn nil? sequential?))

(s/def ::parsed-args
  (s/keys :opt-un [:toucan2.query.parsed-args/kv-args
                   :toucan2.query.parsed-args/columns]))

(defn validate-parsed-args [parsed-args]
  (let [result (s/conform ::parsed-args parsed-args)]
    (when (s/invalid? result)
      (throw (ex-info (format "Invalid parsed args: %s" (s/explain-str ::parsed-args parsed-args))
                      (s/explain-data ::parsed-args parsed-args))))))

(m/defmethod parse-args :around :default
  [query-type model unparsed-args]
  (doto (u/with-debug-result [(list `parse-args query-type model unparsed-args)]
          (next-method query-type model unparsed-args))
    validate-parsed-args))

(m/defmethod parse-args :default
  [query-type model unparsed-args]
  (let [spec   (args-spec query-type model)
        parsed (s/conform spec unparsed-args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret %s args for model %s: %s"
                              (u/safe-pr-str query-type)
                              (u/safe-pr-str model)
                              (s/explain-str spec unparsed-args))
                      (s/explain-data spec unparsed-args))))
    (if-not (map? parsed)
      parsed
      (cond-> parsed
        (not (contains? parsed :queryable)) (assoc :queryable {})
        (seq (:kv-args parsed))             (update :kv-args (fn [kv-args]
                                                               (into {} (map (juxt :k :v)) kv-args)))))))

;;;; [[do-with-query]] and [[with-query]]

(m/defmulti do-with-query
  "Impls should resolve `queryable` to a query and call

    (f query)"
  {:arglists '([model queryable f])}
  u/dispatch-on-first-two-args)

(m/defmethod do-with-query :default
  [_model queryable f]
  (let [f* (^:once fn* [query]
            (assert (not (:queryable query))
                    "Don't pass parsed-args to do-with-query. Pass just the queryable")
            (when (and u/*debug*
                       (not= query queryable))
              (u/with-debug-result ["%s: resolved queryable %s" `do-with-query queryable]
                query))
            (f query))]
    (f* queryable)))

(defmacro with-query [[query-binding [model queryable]] & body]
  `(do-with-query ~model ~queryable (^:once fn* [~query-binding] ~@body)))

;;;; [[build]]

;;; TODO -- it's a little weird that literally every other multimethod in the query execution/compilation pipeline is
;;; `do-with-` while this one isn't. Should this be `do-with-built-query` or something like that?
;;;
;;; TODO -- I'm 99% sure that `query` should be a separate arg from the rest of the `parsed-args`.
(m/defmulti build
  "`build` takes the parsed args returned by [[parse]] and builds them into a query that can be compiled
  by [[toucan2.compile/with-compiled-query]]. For the default implementations, `build` takes the parsed arguments and
  builds a Honey SQL 2 map. Dispatches on `query-type`, `model`, and the `:query` in `parsed-args`."
  {:arglists '([query-type model {:keys [query], :as parsed-args}])}
  (fn [query-type model parsed-args]
    (mapv protocols/dispatch-value [query-type
                            model
                            (:query parsed-args)])))

(m/defmethod build :around :default
  [query-type model parsed-args]
  (assert (map? parsed-args)
          (format "%s expects map parsed-args, got %s." `build (u/safe-pr-str parsed-args)))
  (try
    (u/with-debug-result [(list `build query-type model parsed-args)]
      (next-method query-type model parsed-args))
    (catch Throwable e
      (throw (ex-info (format "Error building %s query for model %s: %s"
                              (u/safe-pr-str query-type)
                              (u/safe-pr-str model)
                              (ex-message e))
                      {:query-type     query-type
                       :model          model
                       :parsed-args    parsed-args
                       :method         #'build
                       :dispatch-value (m/dispatch-value build query-type model parsed-args)}
                      e)))))

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
(m/defmethod build [:default :default nil]
  [query-type model parsed-args]
  (let [parsed-args (cond-> (assoc parsed-args :query {})
                      ;; if `:query` is present but equal to `nil`, treat that as if the pk value IS NULL
                      (contains? parsed-args :query)
                      (update :kv-args assoc :toucan/pk nil))]
    (build query-type model parsed-args)))

(m/defmethod build [:default :default Integer]
  [query-type model parsed-args]
  (build query-type model (update parsed-args :query long)))

(m/defmethod build [:default :default Long]
  [query-type model {pk :query, :as parsed-args}]
  (build query-type model (-> parsed-args
                              (assoc :query {})
                              (update :kv-args assoc :toucan/pk pk))))

(m/defmethod build [:default :default String]
  [query-type model parsed-args]
  (build query-type model (update parsed-args :query (fn [sql]
                                                       [sql]))))

(m/defmethod build [:default :default clojure.lang.Sequential]
  [query-type model {sql-args :query, :keys [kv-args], :as parsed-args}]
  (when (seq kv-args)
    (throw (ex-info "key-value args are not supported for plain SQL queries."
                    {:query-type     query-type
                     :model          model
                     :parsed-args    parsed-args
                     :method         #'build
                     :dispatch-value (m/dispatch-value build query-type model parsed-args)})))
  sql-args)

;;;; Default [[build]] impl for maps; applying key-value args.

(m/defmulti apply-kv-arg
  {:arglists '([model query k v])}
  u/dispatch-on-first-three-args)

(defn condition->honeysql-where-clause [k v]
  (if (sequential? v)
    (vec (list* (first v) k (rest v)))
    [:= k v]))

(m/defmethod apply-kv-arg [:default clojure.lang.IPersistentMap :default]
  [_model honeysql k v]
  (update honeysql :where (fn [existing-where]
                            (:where (hsql.helpers/where existing-where
                                                        (condition->honeysql-where-clause k v))))))

(m/defmethod apply-kv-arg [:default clojure.lang.IPersistentMap :toucan/pk]
  [model honeysql _k v]
  (let [pk-columns (model/primary-keys model)
        v          (if (sequential? v)
                     v
                     [v])]
    (assert (= (count pk-columns)
               (count v))
            (format "Expected %s primary key values for %s, got %d values %s"
                    (count pk-columns) (u/safe-pr-str pk-columns)
                    (count v) (u/safe-pr-str v)))
    (reduce
     (fn [honeysql [k v]]
       (apply-kv-arg model honeysql k v))
     honeysql
     (zipmap pk-columns v))))

(defn apply-kv-args [model query kv-args]
  (reduce
   (fn [query [k v]]
     (apply-kv-arg model query k v))
   query
   kv-args))

(m/defmethod build [:default :default clojure.lang.IPersistentMap]
  [_query-type model {:keys [kv-args query], :as _args}]
  (apply-kv-args model query kv-args))
