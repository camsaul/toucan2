(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]))

(m/defmethod query/build [::delete :default clojure.lang.IPersistentMap]
  [query-type model args]
  (let [args (update args :query (fn [query]
                                   (merge {:delete-from [(keyword (model/table-name model))]}
                                          query)))]
    (next-method query-type model args)))

(defn reducible-delete
  {:arglists '([modelable & conditions? query?])}
  [modelable & unparsed-args]
  (op/reducible ::delete modelable unparsed-args))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [modelable & unparsed-args]
  (op/returning-update-count! ::delete modelable unparsed-args))
