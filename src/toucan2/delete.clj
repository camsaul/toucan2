(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]))

(m/defmethod query/build [::delete :default clojure.lang.IPersistentMap]
  [query-type model parsed-args]
  (let [parsed-args (update parsed-args :query (fn [query]
                                                 (merge {:delete-from [(keyword (model/table-name model))]}
                                                        query)))]
    (next-method query-type model parsed-args)))

(defn reducible-delete
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (op/reducible-update ::delete unparsed-args))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (op/returning-update-count! ::delete unparsed-args))
