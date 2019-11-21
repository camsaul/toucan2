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

(defn query
  ([]
   (query nil nil nil))

  ([model]
   (query model nil nil))

  ([model honeysql-form]
   (query model honeysql-form nil))

  ([model honeysql-form options]
   (types/->Query (types/model model) honeysql-form options nil)))

(defn- select-pk [query pk-values]
  (h/merge-where query {:where (compile/primary-key-where-clause query pk-values)}))

(defn select
  {:arglists '([model-or-object pk-value-or-honeysql-form? & options])}
  ([x]
   (cond
     (types/instance? x) (select-pk (query x) (compile/primary-key-value x))
     (types/query? x)    (query x x)
     (sequential? x)     (assoc (query (first x)) :fields (rest x))
     :else               (query x)))

  ([model x]
   (if (map? x)
     (merge (query model) x)
     (select-pk (query model) x)))

  ([model k v & more]
   (merge (query model) (compile/compile-select-options (into [k v] more)))
   ;; TODO - Compile the optional where args
   ))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                 TOUCAN 2 STUFF                                                 |
;;; +----------------------------------------------------------------------------------------------------------------+
