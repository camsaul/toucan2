(ns bluejdbc.table-aware
  (:refer-clojure :exclude [count])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.result-set :as rs]
            [bluejdbc.table-aware.specs :as specs]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [honeysql.helpers :as hsql.helpers]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(defn- table-rf [tableable]
  (u/pretty-printable-fn
   #(list `table-rf tableable)
   ((map (partial into (instance/instance tableable))) conj)))

(defn reducible-query-as [connectable tableable queryable options]
  (let [options (u/recursive-merge (conn/default-options connectable)
                                   options
                                   {:execute {:builder-fn (rs/row-builder-fn connectable tableable)}})]
    (query/reducible-query connectable tableable queryable options)))

(m/defmulti select*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod select* :default
  [connectable tableable query options]
  (reducible-query-as connectable tableable (compile/from connectable tableable query options) options))

(m/defmethod select* [:default :default nil]
  [connectable tableable _ options]
  (next-method connectable tableable {} options))

(m/defmethod select* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable query options]
  (let [query (merge {:select [:*]} query)]
    (next-method connectable tableable query options)))

(m/defmulti parse-select-args
  {:arglists '([connectable tableable args])}
  u/dispatch-on-first-two-args)

(m/defmethod parse-select-args :around :default
  [connectable tableable args]
  (log/tracef "Parsing select args for %s %s" tableable (pr-str args))
  (let [parsed (next-method connectable tableable args)]
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    parsed))

(m/defmethod parse-select-args :default
  [connectable tableable args]
  (let [spec   (specs/select-args-spec connectable tableable)
        parsed (s/conform spec args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain spec args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (let [{[_ {:keys [id query kvs]}] :query, :keys [options]} parsed]
      {:id      id
       :kvs     (when (seq kvs)
                  (zipmap (map :k kvs) (map :v kvs)))
       ;; TODO -- should probably be `:queryable` instead of `:query` for clarity.
       :query   query
       :options options})))

(defn- merge-primary-key [kvs connectable tableable pk-vals]
  (log/tracef "Adding primary key values %s" (pr-str pk-vals))
  (let [pk-cols (tableable/primary-key connectable tableable)
        _       (log/tracef "Primary key(s) for %s is %s" (pr-str tableable) (pr-str pk-cols))
        pk-cols (if (sequential? pk-cols)
                  pk-cols
                  [pk-cols])
        pk-vals (if (sequential? pk-vals)
                  pk-vals
                  [pk-vals])
        pk-map  (zipmap pk-cols pk-vals)
        result  (merge kvs pk-map)]
    (log/tracef "-> %s" (pr-str result))
    result))

(defn- merge-kvs [query kvs]
  (log/tracef "Adding key-values %s" (pr-str kvs))
  (let [query (apply hsql.helpers/merge-where query (for [[k v] kvs]
                                                      (if (sequential? v)
                                                        (into [(first v) k] (rest v))
                                                        [:= k v])))]
    (log/tracef "-> %s" (pr-str query))
    query))

(defn select-reducible
  {:arglists '([tableable id? conditions? queryable? options?]
               [[connectable tableable] id? conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable]        (if (sequential? connectable-tableable)
                                         connectable-tableable
                                         [conn/*connectable* connectable-tableable])
        {:keys [id kvs query options]} (parse-select-args connectable tableable args)
        options                        (u/recursive-merge (conn/default-options connectable) options)
        kvs                            (cond-> kvs
                                         id (merge-primary-key connectable tableable id))
        query                          (if query
                                         (queryable/queryable connectable tableable query options)
                                         {})
        query                          (cond-> query
                                         (seq kvs) (merge-kvs kvs))]
    (select* connectable tableable query options)))

(def ^{:arglists '([tableable id? conditions? queryable? options?]
                   [[connectable tableable] id? conditions? queryable? options?])} select
  (comp query/all select-reducible))

(def ^{:arglists '([tableable id? conditions? queryable? options?]
                   [[connectable tableable] id? conditions? queryable? options?])} select-one
  (comp (partial transduce
                 (comp (map query/realize-row)
                       (take 1))
                 (completing (fn [_ row] row))
                 nil)
        select-reducible))

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
                                                  id (merge-primary-key connectable tableable id))
        honeysql-form                           (cond-> {:update (compile/table-identifier tableable options)
                                                         :set    changes}
                                                  (seq conditions) (merge-kvs conditions))]
    (log/tracef "UPDATE %s SET %s WHERE %s" (pr-str tableable) (pr-str changes) (pr-str conditions))
    (query/execute! connectable tableable honeysql-form options)))

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

;; TODO

(defn select-one-field [])

(defn select-one-id [])

(defn select-field [])

(defn select-ids [])

(defn select-field->field [])

(defn select-field->id [])

(defn insert! [])

(defn insert-returning-keys! [])

(defn delete! [])

(defn upsert! [])

(defn count [])

(defn exists? [])
