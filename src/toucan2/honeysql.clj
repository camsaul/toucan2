(ns toucan2.honeysql
  "Convenience namespace for loading all HoneySQL integration namespaces."
  (:require [methodical.core :as m]
            toucan2.honeysql.build-query
            toucan2.honeysql.compile
            [toucan2.query :as query]
            [toucan2.queryable :as queryable]
            [toucan2.select :as select]
            [toucan2.util :as u]))

(comment toucan2.honeysql.build-query/keep-me
         toucan2.honeysql.compile/keep-me)

(derive :toucan2/honeysql :toucan2/buildable-query)

(defn honeysql-query [m]
  (vary-meta m assoc :type :toucan2/honeysql))

(defn maybe-add-honeysql-query-metadata [query]
  (cond-> query
    (isa? (u/dispatch-value query) clojure.lang.IPersistentMap)
    honeysql-query))

;; if either the connectable or the tableable derive from `:toucan2/honeysql`, then we should treat plain maps as
;; HoneySQL queries.
(m/defmethod queryable/queryable* [:default :toucan2/honeysql clojure.lang.IPersistentMap]
  [connectable tableable queryable options]
  (queryable/queryable* connectable tableable (honeysql-query queryable) options))

(m/defmethod queryable/queryable* [:toucan2/honeysql :default clojure.lang.IPersistentMap]
  [connectable tableable queryable options]
  (queryable/queryable* connectable tableable (honeysql-query queryable) options))

(m/defmethod queryable/queryable* :after [:default :toucan2/honeysql :default]
  [_ _ query _]
  (maybe-add-honeysql-query-metadata query))

(m/defmethod queryable/queryable* :after [:toucan2/honeysql :default :default]
  [_ _ query _]
  (maybe-add-honeysql-query-metadata query))

(m/defmethod select/count* [:default :default :toucan2/honeysql]
  [connectable tableable honeysql-form options]
  (query/reduce-first
   (map :count)
   (select/select* connectable tableable (assoc honeysql-form :select [[:%count.* :count]]) options)))

;; TODO -- it seems like it would be a lot more efficient if we could use some bespoke JDBC code here e.g. for example
;; simply checking whether the `ResultSet` returns next (rather than fetching the row in the first place)
;;
;; At least we're avoiding the overhead of creating an actual row map since we're only fetching a single column.
(m/defmethod select/exists?* [:default :default :toucan2/honeysql]
  [connectable tableable honeysql-form options]
  (boolean
   (query/reduce-first
    (map :one)
    (select/select* connectable tableable (assoc honeysql-form :select [[1 :one]], :limit 1) options))))
