(ns bluejdbc.select
  "Table-aware methods for fetching data from the database. `select` and related methods."
  (:refer-clojure :exclude [count])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.result-set :as rs]
            [bluejdbc.specs :as specs]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(defn- table-rf [tableable]
  (u/pretty-printable-fn
   #(list `table-rf tableable)
   ((map (partial into (instance/instance tableable))) conj)))

;; TODO -- consider whether this should be moved to `query`
(defn reducible-query-as
  ([tableable queryable]
   (reducible-query-as conn/*connectable* tableable queryable nil))

  ([connectable tableable queryable]
   (reducible-query-as connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [options (u/recursive-merge (conn/default-options connectable)
                                    options
                                    {:execute {:builder-fn (rs/row-builder-fn connectable tableable)}})]
     (query/reducible-query connectable tableable queryable options))))

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

(m/defmulti parse-select-args*
  {:arglists '([connectable tableable args options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-select-args* :around :default
  [connectable tableable args options]
  (log/tracef "Parsing select args for %s %s" tableable (pr-str args))
  (let [parsed (next-method connectable tableable args options)]
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    parsed))

(m/defmethod parse-select-args* :default
  [connectable tableable args _]
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

(defn select-reducible
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable]        (conn/parse-connectable-tableable connectable-tableable)
        {:keys [id kvs query options]} (parse-select-args* connectable tableable args (conn/default-options connectable))
        options                        (u/recursive-merge (conn/default-options connectable) options)
        kvs                            (cond-> kvs
                                         id (honeysql-util/merge-primary-key connectable tableable id))
        query                          (if query
                                         (queryable/queryable connectable tableable query options)
                                         {})
        query                          (cond-> query
                                         (seq kvs) (honeysql-util/merge-kvs kvs))]
    (select* connectable tableable query options)))

(defn select
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (query/all (apply select-reducible args)))

(defn select-one
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (query/reduce-first (map query/realize-row) (apply select-reducible args)))

(defn select-fn-reducible
  {:arglists '([f connectable-tableable pk? conditions? queryable? options?])}
  [f & args]
  (eduction
   (map f)
   (apply select-reducible args)))

(defn select-fn-set
  {:arglists '([f connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (reduce conj #{} (apply select-fn-reducible args)))

(defn select-fn-vec
  {:arglists '([f connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (reduce conj [] (apply select-fn-reducible args)))

(defn select-one-fn
  {:arglists '([f connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-fn-reducible args)))

(defn select-pks-reducible
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        pk-keys                 (tableable/primary-key-keys connectable tableable)
        f                       (if (= (clojure.core/count pk-keys) 1)
                                  (first pk-keys)
                                  (apply juxt pk-keys))]
    (apply select-fn-reducible f [connectable tableable] args)))

(defn select-pks-set
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (reduce conj #{} (apply select-pks-reducible args)))

(defn select-pks-vec
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (reduce conj [] (apply select-pks-reducible args)))

(defn select-one-pk
  {:arglists '([connectable-tableable pk? conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-pks-reducible args)))

(defn select-fn->fn [])

(defn select-fn->pk [])

(defn select-pk->fn [])

(defn count [])

(defn exists? [])
