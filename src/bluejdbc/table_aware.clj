(ns bluejdbc.table-aware
  (:refer-clojure :exclude [count])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.result-set :as rs]
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

(defn- select-args-spec [connectable tableable]
  (letfn [(query? [x]
            (or (map? x)
                (queryable/queryable? connectable tableable x)))]
    (s/cat :query   (s/alt :map     (s/cat :id    (s/? (every-pred (complement map?) (complement keyword?)))
                                           :kvs   (s/* (s/cat
                                                        :k keyword?
                                                        :v (complement map?)))
                                           :query (s/? query?))

                           :non-map (s/cat :query (s/? (complement query?))))
           :options (s/? map?))))

(m/defmethod parse-select-args :default
  [connectable tableable args]
  (let [parsed (s/conform (select-args-spec connectable tableable) args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain ::select-args args))
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
  {:arglists '([tableable id? kvs? queryable? options?]
               [[connectable tableable] id? kvs? queryable? options?])}
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

(def ^{:arglists '([tableable id? kvs? queryable? options?]
                   [[connectable tableable] id? kvs? queryable? options?])} select
  (comp query/all select-reducible))

;; TODO

(defn select-one [])

(defn select-one-field [])

(defn select-one-id [])

(defn select-field [])

(defn select-ids [])

(defn select-field->field [])

(defn select-field->id [])

(defn insert! [])

(defn insert-returning-keys! [])

(defn update! [])

(defn delete! [])

(defn upsert! [])

(defn save! [])

(defn count [])

(defn exists? [])
