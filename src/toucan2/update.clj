(ns toucan2.update
  "Implementation of `update!`."
  (:require [methodical.core :as m]
            [toucan2.util :as u]
            [clojure.spec.alpha :as s]
            [toucan2.query :as query]
            [toucan2.model :as model]
            [toucan2.connection :as conn]))

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

(m/defmulti build-update-query
  "Return a query that can be executed with The value of `args` depends on what [[parse-args]] returns for the model. This
should return an empty query if there are no changes to perform."
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod build-update-query :after :default
  [_model query]
  (u/println-debug (format "Built update query: %s" (pr-str query)))
  query)

(m/defmethod build-update-query :default
  [model {:keys [conditions changes]}]
  (when (seq changes)
    (let [honeysql {:update [(keyword (model/table-name model))]
                    :set    changes}]
      (model/apply-conditions model honeysql conditions))))

(m/defmulti update!*
  "The value of `args` depends on what [[parse-args]] returns for the model."
  {:arglists '([model args])}
  u/dispatch-on-first-two-args)

(m/defmethod update!* :around :default
  [model args]
  (u/with-debug-result (pr-str (list 'update!* model args))
    (next-method model args)))

(m/defmethod update!* :default
  [model args]
  (let [query (build-update-query model args)]
    (if (empty? query)
      (do
        (u/println-debug "Query has no changes, skipping update")
        0)
      (let [result (query/execute! (model/default-connectable model) query)]
        (or (-> result first :next.jdbc/update-count)
            result)))))

(defn update!
  "Returns number of rows updated."
  {:arglists '([modelable pk? conditions-map? & kv-conditions changes-map])}
  [modelable & args]
  (u/with-debug-result (pr-str (list* 'update! modelable args))
    (model/with-model [model modelable]
      (update!* model (parse-args model args)))))
