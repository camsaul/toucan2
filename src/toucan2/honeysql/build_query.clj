(ns toucan2.honeysql.build-query
  (:require [clojure.string :as str]
            [honeysql.helpers :as hsql.helpers]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.build-query :as build-query]
            [toucan2.compile :as compile]
            [toucan2.honeysql.compile :as honeysql.compile]
            [toucan2.log :as log]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

(m/defmethod build-query/conditions* [:default :default :toucan2/honeysql]
  [_ _ query]
  (:where query))

(m/defmethod build-query/with-conditions* [:default :default :toucan2/honeysql]
  [_ _ query new-conditions _]
  (assoc query :where new-conditions))

;; TODO -- this should take an additional `query` arg and dispatch on that and `k` as well.
(m/defmulti handle-sequential-condition*
  {:arglists '([connectableᵈ tableableᵈ k [conditon-typeᵈ :as conditionᵗ] options])}
  (fn [connectable tableable _ [condition-type] _]
    (u/dispatch-on-first-three-args connectable tableable condition-type))
  :combo (m.combo.threaded/threading-method-combination :fourth))

(m/defmethod handle-sequential-condition* :default
  [connectable tableable k [condition-type & args] options]
  (into [condition-type k] (map
                            (fn ->value [arg]
                              (if (sequential? arg)
                                (mapv ->value arg)
                                (compile/maybe-wrap-value connectable tableable k arg options)))
                            args)))

(m/defmulti handle-condition*
  {:arglists '([connectableᵈ tableableᵈ kᵈ conditionᵈᵗ options])}
  u/dispatch-on-first-four-args
  :combo (m.combo.threaded/threading-method-combination :fourth))

(m/defmethod handle-condition* :default
  [connectable tableable k v options]
  (when (namespace k)
    (throw (ex-info (format "Don't know how to handle condition %s. Did you mean %s? Consider adding an impl for %s for %s"
                            k
                            (str/join " or " (for [dispatch-value (keys (m/primary-methods handle-condition*))
                                                   :when          (sequential? dispatch-value)
                                                   :let           [[_ _ condition] dispatch-value]
                                                   :when          (not= condition :default)]
                                               condition))
                            `handle-condition*
                            [:default :default k :default])
                    {:k              k
                     :v              v
                     :dispatch-value (m/dispatch-value handle-condition* connectable tableable k v)})))
  [:= k (compile/maybe-wrap-value connectable tableable k v options)])

(m/defmethod handle-condition* :around :default
  [connectable tableable k v options]
  (log/with-trace ["Add condition %s %s (dispatch value = %s)"
                   k v (m/dispatch-value handle-condition* connectable tableable k v)]
    (next-method connectable tableable k v options)))

(m/defmethod handle-condition* [:default :default :default clojure.lang.Sequential]
  [connectable tableable k v options]
  (handle-sequential-condition* connectable tableable k v options))

(m/defmethod handle-condition* [:default :default :toucan2/with-pks :default]
  [connectable tableable _ pks options]
  (when (seq pks)
    (let [pk-keys (tableable/primary-key-keys connectable tableable)
          pks     (for [pk-vals pks]
                    (if (sequential? pk-vals)
                      pk-vals
                      [pk-vals]))]
      (log/with-trace ["Adding :toucan2/with-pks %s condition" pks]
        (into [:and] (map-indexed
                      (fn [i k]
                        (let [vs (mapv #(nth % i) pks)]
                          [:in k vs]))
                      pk-keys))))))

(m/defmethod build-query/merge-kv-conditions* [:default :default :toucan2/honeysql]
  [connectable tableable query kv-conditions options]
  (log/with-trace ["Adding key-value conditions %s" kv-conditions]
    (apply hsql.helpers/merge-where query (for [[k v] kv-conditions]
                                            (handle-condition* connectable tableable k v options)))))

(m/defmethod build-query/rows* [:default :default :toucan2/honeysql]
  [_ _ query]
  (:values query))

(m/defmethod build-query/with-rows* [:default :default :toucan2/honeysql]
  [connectable tableable query new-rows options]
  (assoc query :values (for [row new-rows]
                         (do
                           (assert (seq row) "Row cannot be empty")
                           (into {} (for [[k v] row]
                                      [k (compile/maybe-wrap-value connectable tableable k v options)]))))))

(m/defmethod build-query/changes* [:default :default :toucan2/honeysql]
  [_ _ query]
  (:set query))

(m/defmethod build-query/with-changes* [:default :default :toucan2/honeysql]
  [_ _ query new-changes _]
  (assoc query :set new-changes))

(derive :toucan2.honeysql/select-query :toucan2/honeysql)
(derive :toucan2.honeysql/update-query :toucan2/honeysql)
(derive :toucan2.honeysql/insert-query :toucan2/honeysql)
(derive :toucan2.honeysql/delete-query :toucan2/honeysql)

(def ^:private empty-select-query
  (vary-meta {:select [:*]} assoc :type :toucan2.honeysql/select-query))

(m/defmethod build-query/select-query* [:default :default :toucan2/honeysql]
  [_ tableable options]
  (assoc empty-select-query :from [(honeysql.compile/table-identifier tableable options)]))

(m/defmethod build-query/table* [:default :default :toucan2.honeysql/select-query]
  [_ _ query]
  (:from query))

(m/defmethod build-query/with-table* [:default :default :toucan2.honeysql/select-query]
  [_ _ query new-table options]
  (let [new-table (cond-> new-table
                    (not (honeysql.compile/table-identifier? new-table)) (honeysql.compile/table-identifier options))]
    (assoc query :from [new-table])))

;; if you don't say otherwise, assume that with-table for `:toucan2/honeysql` means a select query
(m/defmethod build-query/with-table* [:default :default :toucan2/honeysql]
  [connectable tableable query new-table options]
  (let [query (vary-meta query assoc :type :toucan2.honeysql/select-query)]
    (build-query/with-table* connectable tableable query new-table options)))

(def ^:private empty-update-query
  (vary-meta {} assoc :type :toucan2.honeysql/update-query))

(m/defmethod build-query/update-query* [:default :default :toucan2/honeysql]
  [_ tableable options]
  (assoc empty-update-query :update (honeysql.compile/table-identifier tableable options)))

(m/defmethod build-query/table* [:default :default :toucan2.honeysql/update-query]
  [_ _ query]
  (:update query))

(m/defmethod build-query/with-table* [:default :default :toucan2.honeysql/update-query]
  [_ _ query new-table _]
  (assoc query :update new-table))

(def ^:private empty-insert-query
  (vary-meta {} assoc :type :toucan2.honeysql/insert-query))

(m/defmethod build-query/insert-query* [:default :default :toucan2/honeysql]
  [_ tableable options]
  (assoc empty-insert-query :insert-into (honeysql.compile/table-identifier tableable options)))

(m/defmethod build-query/table* [:default :default :toucan2.honeysql/insert-query]
  [_ _ query]
  (:insert-into query))

(m/defmethod build-query/with-table* [:default :default :toucan2.honeysql/insert-query]
  [_ _ query new-table _]
  (assoc query :insert-into new-table))

(def ^:private empty-delete-query
  (vary-meta {} assoc :type :toucan2.honeysql/delete-query))

(m/defmethod build-query/delete-query* [:default :default :toucan2/honeysql]
  [_ tableable options]
  (assoc empty-delete-query :delete-from (honeysql.compile/table-identifier tableable options)))

(m/defmethod build-query/table* [:default :default :toucan2.honeysql/delete-query]
  [_ _ query]
  (:delete-from query))

(m/defmethod build-query/with-table* [:default :default :toucan2.honeysql/delete-query]
  [_ _ query new-table _]
  (assoc query :delete-from new-table))
