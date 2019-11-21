(ns fourcan.db
  (:require [fourcan
             [types :as types]]
            [fourcan.db.jdbc :as jdbc]
            [methodical.core :as m]
            [honeysql.helpers :as h]
            [fourcan.compile :as compile]))

(m/defmethod types/invoke-query :default
  [query]
  (jdbc/query (types/model query) (types/honeysql-form query) (types/query-options query)))

(defn- select-pk [query pk-values]
  (h/merge-where query (compile/primary-key-where-clause query pk-values)))

(defn select
  {:arglists '([model-or-object pk-value-or-honeysql-form? & options])}
  ([x]
   (cond
     (types/query? x)    (types/query x x)
     (types/instance? x) (select-pk (select (types/model x)) (compile/primary-key-value x))
     (sequential? x)     (assoc (types/query (first x)) :fields (rest x))
     :else               (types/query x {:select [:*], :from [(types/model x)]})))

  ([model x]
   (if (map? x)
     (merge (select model) x)
     (select-pk (select model) x)))

  ([model k v & more]
   ;; TODO - have `compile-select-options` return where clause, not map
   (h/merge-where (types/query model) (:where (compile/compile-select-options (into [k v] more))))
   ;; TODO - Compile the optional where args
   ))
