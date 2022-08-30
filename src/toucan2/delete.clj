(ns toucan2.delete
  "Implementation of [[delete!]]."
  (:require
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

(m/defmethod pipeline/transduce-resolved-query [#_query-type :toucan.query-type/delete.*
                                                #_model      :default
                                                #_query      clojure.lang.IPersistentMap]
  [rf query-type model parsed-args resolved-query]
  (let [resolved-query (merge {:delete-from (query/honeysql-table-and-alias model)}
                              resolved-query)]
    (next-method rf query-type model parsed-args resolved-query)))

(defn reducible-delete
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/delete.update-count unparsed-args))

(defn delete!
  "Returns number of rows deleted."
  {:arglists '([modelable & conditions? query?])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/delete.update-count unparsed-args))
