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
            [clojure.spec.alpha :as s]))

(defn parse-update!-args [args]
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

(defn update!
  {:arglists '([tableable id? conditions? changes options?])}
  [connectable-tableable & args]
  (let [[connectable tableable]                 (if (sequential? connectable-tableable)
                                                  connectable-tableable
                                                  [conn/*connectable* connectable-tableable])
        {:keys [id conditions changes options]} (parse-update!-args args)
        conditions                              (cond-> conditions
                                                  id (honeysql-util/merge-primary-key connectable tableable id))
        honeysql-form                           (cond-> {:update (compile/table-identifier tableable options)
                                                         :set    changes}
                                                  (seq conditions) (honeysql-util/merge-kvs conditions))]
    (log/tracef "UPDATE %s SET %s WHERE %s" (pr-str tableable) (pr-str changes) (pr-str conditions))
    (query/execute! connectable tableable honeysql-form options)))

;; TODO -- Move to tableable
(defn primary-key-values [connectable obj]
  (let [pk-keys (tableable/primary-key connectable (instance/table obj))
        pk-keys (if (sequential? pk-keys)
                  pk-keys
                  [pk-keys])]
    (zipmap pk-keys (map obj pk-keys))))

(defn save!
  ([obj]
   (save! conn/*connectable* obj))

  ([connectable obj]
   (when-let [changes (not-empty (instance/changes obj))]
     (update! (instance/table obj) (primary-key-values connectable obj) changes))))



(defn insert! [])

(defn insert-returning-keys! [])

(defn delete! [])

(defn upsert! [])
