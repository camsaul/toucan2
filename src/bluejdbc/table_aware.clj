(ns bluejdbc.table-aware
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as rs]
            [bluejdbc.util :as u]))

(defn query-as [connectable tableable query options]
  (let [options (u/recursive-merge (conn/default-options connectable)
                                   {:execute {:builder-fn (rs/row-builder-fn connectable tableable)}}
                                   ;; TODO -- not sure why we need to do *both* ???
                                   {:rf ((map (partial into (instance/instance tableable))) conj)}
                                   options)]
    (query/query connectable query options)))

(defn basic-select [connectable tableable query options]
  (query-as connectable tableable (compile/from connectable tableable query options) options))
