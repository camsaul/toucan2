(ns bluejdbc.mutative
  "Table-aware methods that for changing data in the database, e.g. `update!`, `insert!`, and `save!`."
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.specs :as specs]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(m/defmulti parse-update!-args*
  {:arglists '([connectable tableable args])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-update!-args* :default
  [_ _ args]
  (let [parsed (s/conform ::specs/update!-args args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret update! args: %s" (s/explain ::specs/update!-args args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (let [{:keys [kv-conditions]} parsed]
      (-> parsed
          (dissoc :kv-conditions)
          (update :conditions merge (when (seq kv-conditions)
                                      (zipmap (map :k kv-conditions) (map :v kv-conditions))))))))

(m/defmulti update!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod update!* :default
  [connectable tableable query options]
  (query/execute! connectable tableable query options))

(defn update!
  {:arglists '([connectable-tableable id? conditions? changes options?])}
  [connectable-tableable & args]
  (let [[connectable tableable]                 (conn/parse-connectable-tableable connectable-tableable)
        {:keys [id conditions changes options]} (parse-update!-args* connectable tableable args)
        conditions                              (cond-> conditions
                                                  id (honeysql-util/merge-primary-key connectable tableable id))
        honeysql-form                           (cond-> {:update (compile/table-identifier tableable options)
                                                         :set    changes}
                                                  (seq conditions) (honeysql-util/merge-kvs conditions))]
    (log/tracef "UPDATE %s SET %s WHERE %s" (pr-str tableable) (pr-str changes) (pr-str conditions))
    (update!* connectable tableable honeysql-form (merge (conn/default-options connectable)
                                                         options))))


(m/defmulti save!*
  {:arglists '([connectable tableable obj options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod save!* :default
  [connectable tableable obj _]
  (when-let [changes (not-empty (instance/changes obj))]
    (update! [connectable tableable] (tableable/primary-key-values connectable tableable obj) changes)))

(defn save!
  [obj]
  (let [connectable (or (instance/connectable obj) conn/*connectable*)
        tableable   (instance/table obj)
        options     (conn/default-options connectable)]
    (save!* connectable tableable obj options)))

(defn insert! [])

(defn insert-returning-keys! [])

(defn delete! [])

(defn upsert! [])
