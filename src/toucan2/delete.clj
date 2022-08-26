(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

(m/defmethod query/build [:toucan.query-type/delete.* :default clojure.lang.IPersistentMap]
  [query-type model parsed-args]
  (let [parsed-args (update parsed-args :query (fn [query]
                                                 (merge {:delete-from [(keyword (model/table-name model))]}
                                                        query)))]
    (next-method query-type model parsed-args)))

(defn reducible-delete
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/delete.update-count unparsed-args))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed :toucan.query-type/delete.update-count unparsed-args))
