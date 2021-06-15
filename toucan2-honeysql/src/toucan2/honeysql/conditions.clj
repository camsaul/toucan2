(ns toucan2.honeysql.conditions
  (:require [clojure.string :as str]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.compile :as compile]
            [toucan2.log :as log]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

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
  (assert (keyword? k) (format "Expected keyword, got %s" (pr-str k)))
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
