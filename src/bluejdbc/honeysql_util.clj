(ns bluejdbc.honeysql-util
  (:require [bluejdbc.log :as log]
            [bluejdbc.tableable :as tableable]
            [honeysql.helpers :as hsql.helpers]))

(defn merge-primary-key [kvs connectable tableable pk-vals]
  (log/with-trace ["Adding primary key values %s" (pr-str pk-vals)]
    (let [pk-cols (tableable/primary-key-keys connectable tableable)
          _       (log/tracef "Primary key(s) for %s is %s" (pr-str tableable) (pr-str pk-cols))
          pk-vals (if (sequential? pk-vals)
                    pk-vals
                    [pk-vals])
          pk-map  (zipmap pk-cols pk-vals)]
      (merge kvs pk-map))))

;; TODO -- this should probably be a multimethod, to support theoretical non-HoneySQL queries
(defn merge-kvs [query kvs]
  (log/with-trace ["Adding key-values %s" (pr-str kvs)]
    (apply hsql.helpers/merge-where query (for [[k v] kvs]
                                            (if (sequential? v)
                                              (into [(first v) k] (rest v))
                                              [:= k v])))))
