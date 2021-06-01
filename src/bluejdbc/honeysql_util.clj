(ns bluejdbc.honeysql-util
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.log :as log]
            [bluejdbc.tableable :as tableable]
            [honeysql.helpers :as hsql.helpers]))

(defn merge-primary-key [kvs connectable tableable pk-vals options]
  (log/with-trace ["Adding primary key values %s" (pr-str pk-vals)]
    (let [pk-cols (tableable/primary-key-keys connectable tableable)
          _       (log/tracef "Primary key(s) for %s is %s" (pr-str tableable) (pr-str pk-cols))
          pk-vals (if (sequential? pk-vals)
                    pk-vals
                    [pk-vals])
          pk-map  (into {} (map
                            (fn [k v]
                              [k (compile/value connectable tableable k v options)])
                            pk-cols
                            pk-vals))]
      (merge kvs pk-map))))

;; TODO -- this should probably be a multimethod, to support theoretical non-HoneySQL queries
(defn merge-conditions [query connectable tableable conditions options]
  (log/with-trace ["Adding key-value conditions %s" conditions]
    (apply hsql.helpers/merge-where query (for [[k v] conditions]
                                            (if (sequential? v)
                                              (into [(first v) k] (map
                                                                   (fn ->value [v]
                                                                     (if (sequential? v)
                                                                       (mapv ->value v)
                                                                       (compile/value connectable tableable k v options)))
                                                                   (rest v)))
                                              [:= k (compile/value connectable tableable k v options)])))))
