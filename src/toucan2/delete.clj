(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

(m/defmethod query/build [#_query-type :toucan.query-type/delete.*
                          #_model      :default
                          #_query      clojure.lang.IPersistentMap]
  [query-type model parsed-args]
  (let [parsed-args (update parsed-args :query (fn [query]
                                                 (merge {:delete-from (query/honeysql-table-and-alias model)}
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
