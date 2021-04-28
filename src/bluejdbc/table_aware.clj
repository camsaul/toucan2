(ns bluejdbc.table-aware
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as rs]))

(defn query-as [connectable tableable query options]
  (conn/with-connection [[conn options] connectable options]
    (let [options (-> (assoc-in options [:execute :builder-fn] (rs/row-builder-fn connectable tableable))
                      ;; TODO -- not sure why we need to do *both*
                      (assoc :rf ((map (partial into (instance/instance tableable))) conj)))]
      (query/query connectable query options))))

(defn x []
  (query-as :test/postgres :people {:select [:*], :from [:people]} nil))
