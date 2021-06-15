(ns toucan2.honeysql.build-query
  (:require [honeysql.helpers :as hsql.helpers]
            [methodical.core :as m]
            [toucan2.build-query :as build-query]
            [toucan2.compile :as compile]
            [toucan2.honeysql.compile :as honeysql.compile]
            [toucan2.honeysql.conditions :as honeysql.conditions]
            [toucan2.log :as log]))

(derive :toucan2.honeysql/select-query :toucan2/honeysql)
(derive :toucan2.honeysql/update-query :toucan2/honeysql)
(derive :toucan2.honeysql/insert-query :toucan2/honeysql)
(derive :toucan2.honeysql/delete-query :toucan2/honeysql)

;; TODO -- these should implement connectable and tableable using metadata

(defn buildable-query-of-type [connectable tableable query query-type]
  (vary-meta query assoc
             :connectable connectable
             :tableable tableable
             :type (case query-type
                     :select :toucan2.honeysql/select-query
                     :update :toucan2.honeysql/update-query
                     :insert :toucan2.honeysql/insert-query
                     :delete :toucan2.honeysql/delete-query)))

(m/defmethod build-query/buildable-query* [:toucan2/honeysql :default clojure.lang.IPersistentMap :default]
  [connectable tableable query query-type _]
  (buildable-query-of-type connectable tableable query query-type))

(m/defmethod build-query/buildable-query* [:default :toucan2/honeysql clojure.lang.IPersistentMap :default]
  [connectable tableable query query-type _]
  (buildable-query-of-type connectable tableable query query-type))

(m/prefer-method!
 #'build-query/buildable-query*
 [:toucan2/honeysql :default clojure.lang.IPersistentMap :default]
 [:default :toucan2/honeysql clojure.lang.IPersistentMap :default])

(m/defmethod build-query/buildable-query* [:default :default :toucan2/honeysql :default]
  [connectable tableable query query-type _]
  (buildable-query-of-type connectable tableable query query-type))

(m/defmethod build-query/conditions* :toucan2/honeysql
  [query]
  (:where query))

(m/defmethod build-query/with-conditions* :toucan2/honeysql
  [query new-conditions _]
  (assoc query :where new-conditions))

(m/defmethod build-query/merge-kv-conditions* :toucan2/honeysql
  [query kv-conditions options]
  (assert (contains? (meta query) :connectable)
          (format "Expected query to have :connectable metadata. Got: %s"
                  (binding [*print-meta* true] (pr-str query))))
  (assert (contains? (meta query) :tableable)
          (format "Expected query to have :tableable metadata. Got: %s"
                  (binding [*print-meta* true] (pr-str query))))
  (let [connectable (:connectable (meta query))
        tableable   (:tableable (meta query))]
    (log/with-trace ["Adding key-value conditions %s" kv-conditions]
      (apply hsql.helpers/merge-where query (for [[k v] kv-conditions]
                                              (honeysql.conditions/handle-condition* connectable tableable k v options))))))

(m/defmethod build-query/rows* :toucan2/honeysql
  [query]
  (:values query))

(m/defmethod build-query/with-rows* :toucan2/honeysql
  [query new-rows options]
  (let [connectable (:connectable (meta query))
        tableable   (:tableable (meta query))]
    (assoc query :values (for [row new-rows]
                           (do
                             (assert (seq row) "Row cannot be empty")
                             (into {} (for [[k v] row]
                                        [k (compile/maybe-wrap-value connectable tableable k v options)])))))))

(m/defmethod build-query/changes* :toucan2/honeysql
  [query]
  (:set query))

(m/defmethod build-query/with-changes* :toucan2/honeysql
  [query new-changes _]
  (assoc query :set new-changes))

(m/defmethod build-query/table* :toucan2.honeysql/select-query
  [query]
  (:from query))

(defn- table-identifier [new-table options]
  (cond-> new-table
    (not (honeysql.compile/table-identifier? new-table)) (honeysql.compile/table-identifier options)))

(m/defmethod build-query/with-table* :toucan2.honeysql/select-query
  [query new-table options]
  (let [new-table (table-identifier new-table options)]
    (with-meta
      (merge {:select [:*]
              :from   [new-table]}
             query)
      (meta query))))

;; if you don't say otherwise, assume that with-table for `:toucan2/honeysql` means a select query
(m/defmethod build-query/with-table* :toucan2/honeysql
  [query new-table options]
  (let [query (vary-meta query assoc :type :toucan2.honeysql/select-query)]
    (build-query/with-table* query new-table options)))

(m/defmethod build-query/table* :toucan2.honeysql/update-query
  [query]
  (:update query))

(m/defmethod build-query/with-table* :toucan2.honeysql/update-query
  [query new-table options]
  (assoc query :update (table-identifier new-table options)))

(m/defmethod build-query/table* :toucan2.honeysql/insert-query
  [query]
  (:insert-into query))

(m/defmethod build-query/with-table* :toucan2.honeysql/insert-query
  [query new-table options]
  (assoc query :insert-into (table-identifier new-table options)))

(m/defmethod build-query/table* :toucan2.honeysql/delete-query
  [query]
  (:delete-from query))

(m/defmethod build-query/with-table* :toucan2.honeysql/delete-query
  [query new-table options]
  (assoc query :delete-from (table-identifier new-table options)))
