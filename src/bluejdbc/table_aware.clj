(ns bluejdbc.table-aware
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.result-set :as rs]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]))

(defn query-as [connectable tableable query options]
  (let [options (u/recursive-merge (conn/default-options connectable)
                                   {:execute {:builder-fn (rs/row-builder-fn connectable tableable)}
                                    ;; TODO -- not sure why we need to do *both* ???
                                    :rf      ((map (partial into (instance/instance tableable))) conj)}
                                   options)]
    (query/query connectable query options)))

(m/defmulti select*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod select* :default
  [connectable tableable query options]
  (query-as connectable tableable (compile/from connectable tableable query options) options))

(p/defrecord+ ID [vs]
  pretty/PrettyPrintable
  (pretty [_]
    (list* `id vs)))

(defn id [& vs]
  (->ID (vec vs)))

(defn id? [x]
  (or (integer? x)
      (instance? ID x)))

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

(defn- merge-primary-key [kvs connectable tableable id])

(defn- merge-kvs [query kvs])

(defn select
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
        query                          (queryable/queryable connectable tableable query options)
        query                          (cond-> query
                                         (seq kvs) (merge-kvs kvs))]
    (select* connectable tableable query options)))
