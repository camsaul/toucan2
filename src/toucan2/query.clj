(ns toucan2.query
  (:require
   [methodical.core :as m]
   [toucan2.util :as u]
   [honey.sql.helpers :as hsql.helpers]
   [toucan2.model :as model]
   [clojure.spec.alpha :as s]
   [toucan2.query :as query]))

;;;; [[do-with-query]] and [[with-query]]

(m/defmulti do-with-query
  "Impls should resolve `queryable` to a query and call

    (f query)"
  {:arglists '([model queryable f])}
  u/dispatch-on-first-two-args)

(m/defmethod do-with-query :default
  [_model queryable f]
  (f queryable))

(defmacro with-query [[query-binding [model queryable]] & body]
  `(do-with-query ~model ~queryable (^:once fn* [~query-binding] ~@body)))

;;;; [[build]]

(m/defmulti build
  "Dispatches on `query-type`, `model`, and the `:query` in `args`."
  {:arglists '([query-type model args])}
  (fn [query-type model args]
    (mapv u/dispatch-value [query-type
                            model
                            (when (map? args)
                              (:query args))])))

(m/defmethod build :around :default
  [query-type model args]
  (try
    (u/with-debug-result (pr-str (list `build query-type model args))
      (next-method query-type model args))
    (catch Throwable e
      (throw (ex-info (format "Error building %s query for model %s: %s"
                              (pr-str query-type)
                              (pr-str model)
                              (ex-message e))
                      {:query-type     query-type
                       :model          model
                       :args           args
                       :method #'build
                       :dispatch-value (m/dispatch-value build query-type model args)}
                      e)))))

(m/defmethod build :default
  [query-type model args]
  (throw (ex-info (format "Don't know how to build a query from args %s. Do you need to implement %s for %s?"
                          (pr-str args)
                          `build
                          (pr-str (m/dispatch-value build query-type model args)))
                  {:query-type     query-type
                   :model          model
                   :args           args
                   :method         #'build
                   :dispatch-value (m/dispatch-value build query-type model args)})))

(m/defmethod build [:default :default nil]
  [query-type model args]
  (assert (map? args)
          (format "Default %s method expects map args, got %s. If you want to use non-map args, implement a method for %s"
                  `build
                  (pr-str args)
                  (pr-str (m/dispatch-value build query-type model args))))
  (build query-type model (assoc args :query {})))

(m/defmethod build [:default :default Long]
  [query-type model {pk :query, :as args}]
  (build query-type model (-> args
                              (assoc :query {})
                              (update :kv-args assoc :toucan/pk pk))))

(m/defmethod build [:default :default String]
  [query-type model args]
  (build query-type model (update args :query (fn [sql]
                                                [sql]))))

(m/defmethod build [:default :default clojure.lang.Sequential]
  [query-type model {sql-args :query, :keys [kv-args], :as args}]
  (when (seq kv-args)
    (throw (ex-info "key-value args are not supported for plain SQL queries."
                    {:query-type     query-type
                     :model          model
                     :args           args
                     :method         #'build
                     :dispatch-value (m/dispatch-value build query-type model args)})))
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
                    (count pk-columns) (pr-str pk-columns)
                    (count v) (pr-str v)))
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

;;;; [[parse-args]]

(m/defmulti args-spec
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

(m/defmulti parse-args
  {:arglists '([query-type model unparsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod parse-args :around :default
  [query-type model unparsed-args]
  (u/with-debug-result (pr-str (list 'parse-args query-type model unparsed-args))
    (next-method query-type model unparsed-args)))

(m/defmethod parse-args :default
  [query-type model unparsed-args]
  (let [spec   (args-spec query-type model)
        parsed (s/conform spec unparsed-args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret %s args for model %s: %s"
                              (pr-str query-type)
                              (pr-str model)
                              (s/explain-str spec unparsed-args))
                      (s/explain-data spec unparsed-args))))
    (if-not (map? parsed)
      parsed
      (cond-> parsed
        (nil? (:queryable parsed)) (assoc :queryable {})
        (seq (:kv-args parsed))    (update :kv-args (fn [kv-args]
                                                      (into {} (map (juxt :k :v)) kv-args)))))))

(defn do-with-parsed-args-with-query
  [query-type model unparsed-args f]
  (let [{:keys [queryable], :as parsed} (parse-args query-type model unparsed-args)]
    (query/with-query [query [model queryable]]
      (f (-> parsed
             (dissoc :queryable)
             (assoc :query query))))))

(defmacro with-parsed-args-with-query
  {:style/indent 1}
  [[parsed-args-binding [query-type model unparsed-args]] & body]
  `(do-with-parsed-args-with-query ~query-type ~model ~unparsed-args (^:once fn* [~parsed-args-binding] ~@body)))
