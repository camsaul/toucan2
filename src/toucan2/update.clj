(ns toucan2.update
  "Implementation of [[update!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(m/defmulti parse-args
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod parse-args :after :default
  [_model parsed]
  (u/println-debug (format "Parsed args: %s" (pr-str parsed)))
  parsed)

(s/def ::default-args
  (s/cat :pk            (s/? (complement map?))
         :conditions    (s/? map?)
         :kv-conditions (s/* (s/cat
                              :k any?
                              :v any?))
         :changes       map?))

(m/defmethod parse-args :default
  [_model args]
  (let [parsed (s/conform ::default-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret update! args: %s" (s/explain-str ::default-args args))
                      (s/explain-data ::default-args args))))
    {:conditions (merge (:conditions parsed)
                        (into {} (map (juxt :k :v)) (:kv-conditions parsed))
                        (when-let [pk (:pk parsed)]
                          {:toucan2/pk pk}))
     :changes    (:changes parsed)}))

(m/defmulti update!*
  "The value of `args` depends on what [[parse-args]] returns for the model."
  {:arglists '([model args])}
  u/dispatch-on-first-two-args)

(m/defmethod update!* :around :default
  [model args]
  (u/with-debug-result (pr-str (list 'update!* model args))
    (next-method model args)))

(defn build-query
  "Default way of building queries for the default impl of [[update!*]]."
  [model {:keys [conditions changes]}]
  (let [honeysql {:update [(keyword (model/table-name model))]
                  :set    changes}
        query    (model/apply-conditions model honeysql conditions)]
    (u/println-debug (format "Built update query: %s" (pr-str query)))
    query))

(m/defmethod update!* :default
  [model {:keys [changes], :as query}]
  (if (empty? changes)
    (do
      (u/println-debug "Query has no changes, skipping update")
      0)
    (let [query (build-query model query)]
      (try
        (let [result (query/execute! (model/default-connectable model) query)]
          (or (-> result first :next.jdbc/update-count)
              result))
        (catch Throwable e
          (throw (ex-info (format "Error updating rows: %s" (ex-message e))
                          {:model model, :query query}
                          e)))))))

(defn update!
  "Returns number of rows updated."
  {:arglists '([modelable pk? conditions-map? & kv-conditions changes-map])}
  [modelable & args]
  (u/with-debug-result (pr-str (list* 'update! modelable args))
    (model/with-model [model modelable]
      (update!* model (parse-args model args)))))
