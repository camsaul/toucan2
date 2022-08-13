(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(m/defmulti parse-args
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod parse-args :after :default
  [_model parsed]
  (u/println-debug (format "Parsed args: %s" (pr-str parsed)))
  parsed)

;;; [[delete!]] args are basically the same as [[select/select]] args for now, with the difference that you're not
;;; allowed to wrap model in a vector to specify the fields to return.
(s/def ::default-args ::select/default-args)

(m/defmethod parse-args :default
  [_model args]
  (let [parsed (s/conform ::default-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret delete args: %s" (s/explain-str ::default-args args))
                      (s/explain-data ::default-args args))))
    (cond-> parsed
      (nil? (:query parsed))     (assoc :query {})
      (seq (:conditions parsed)) (update :conditions (fn [conditions]
                                                       (into {} (map (juxt :k :v)) conditions))))))

(m/defmulti delete!*
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod delete!* :around :default
  [model args]
  (u/with-debug-result (pr-str (list 'delete!* model args))
    (next-method model args)))

(m/defmulti build-query
  {:arglists '([model args])}
  (fn [model {:keys [query], :as _args}]
    (u/dispatch-on-first-two-args model query)))

(m/defmethod build-query :around :default
  [model args]
  (u/with-debug-result (pr-str (list `build-query model args))
    (next-method model args)))

(m/defmethod build-query :default
  [model args]
  (throw (ex-info (format "Don't know how to build a delete query for %s from args %s. Do you need to implement %s for %s?"
                          (pr-str model)
                          (pr-str args)
                          `build-query
                          (pr-str (m/dispatch-value build-query model args)))
                  {:model model, :args args})))

(m/defmethod build-query [:default nil]
  [model args]
  (build-query model (assoc args :query {})))

(m/defmethod build-query [:default clojure.lang.IPersistentMap]
  [model {:keys [conditions query], :as _args}]
  (let [honeysql (merge {:delete-from [(keyword (model/table-name model))]}
                        query)]
    (compile/apply-conditions model honeysql conditions)))

(m/defmethod build-query [:default Long]
  [model {pk :query, :as args}]
  (build-query model (-> args
                         (assoc :query {})
                         (update :conditions assoc :toucan/pk pk))))

(m/defmethod delete!* :default
  [model args]
  (let [query (build-query model args)]
    (try
      (let [result (query/execute! (model/current-connectable model) query)]
        (or (-> result first :next.jdbc/update-count)
            result))
      (catch Throwable e
        (throw (ex-info (format "Error deleting rows: %s" (ex-message e))
                        {:model model, :query query}
                        e))))))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (u/with-debug-result (pr-str (list* 'delete! modelable args))
    (model/with-model [model modelable]
      (delete!* model (parse-args model args)))))
