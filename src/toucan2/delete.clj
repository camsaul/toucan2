(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(m/defmethod query/build [::delete :default clojure.lang.IPersistentMap]
  [query-type model args]
  (let [args (update args :query (fn [query]
                                   (merge {:delete-from [(keyword (model/table-name model))]}
                                          query)))]
    (next-method query-type model args)))

(m/defmulti delete!*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod delete!* :around :default
  [model parsed-args]
  (u/with-debug-result (pr-str (list 'delete!* model parsed-args))
    (next-method model parsed-args)))

(m/defmethod delete!* :default
  [model parsed-args]
  (let [query (query/build ::delete model parsed-args)]
    (try
      (execute/query-one (model/deferred-current-connectable model)
                         model
                         query)
      (catch Throwable e
        (throw (ex-info (format "Error deleting rows: %s" (ex-message e))
                        {:model model, :query query}
                        e))))))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [modelable & unparsed-args]
  (u/with-debug-result (pr-str (list* 'delete! modelable unparsed-args))
    (model/with-model [model modelable]
      (query/with-parsed-args-with-query [parsed-args [::delete model unparsed-args]]
        (delete!* model parsed-args)))))
