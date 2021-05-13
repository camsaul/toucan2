(ns bluejdbc.specs
  (:require [bluejdbc.queryable :as queryable]
            [clojure.spec.alpha :as s]))

(s/def ::id
  (every-pred (complement map?) (complement keyword?)))

(s/def ::kvs
  (s/* (s/cat
        :k keyword?
        :v (complement map?))))

(s/def ::options
  map?)

(defn select-args-spec [connectable tableable]
  (letfn [(query? [x]
            (or (map? x)
                (queryable/queryable? connectable tableable x)))]
    (s/cat :query   (s/alt :map     (s/cat :id    (s/? ::id)
                                           :kvs   ::kvs
                                           :query (s/? query?))

                           :non-map (s/cat :query (s/? (complement query?))))
           :options (s/? ::options))))

(s/def ::update!-args
  (s/cat :id            (s/? ::id)
         :conditions    (s/? map?)
         :kv-conditions ::kvs
         :changes       map?
         :options       (s/? ::options)))

(s/def ::insert!-args
  (s/cat :rows (s/alt :single-row-map    map?
                      :multiple-row-maps (s/spec (s/+ map?))
                      :kv-pairs          ::kvs
                      :columns-rows      (s/cat :columns (s/spec (s/+ keyword?))
                                                :rows    (s/spec (s/+ vector?))))
         :options (s/? ::options)))
