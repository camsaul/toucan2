(ns bluejdbc.specs
  (:require [bluejdbc.queryable :as queryable]
            [clojure.spec.alpha :as s]))

(s/def ::pk
  (every-pred (complement map?) (complement keyword?)))

(s/def ::kv-conditions
  (s/* (s/cat
        :k keyword?
        :v (complement map?))))

(s/def ::options
  map?)

;; TODO -- consider whether it makes sense for these to go here as opposed to next to where they're actually used.

(defn select-args-spec [connectable tableable]
  (letfn [(query? [x]
            (or (map? x)
                (queryable/queryable? connectable tableable x)))]
    ;; TODO -- rename these keys, since query is not necessarily a map.
    (s/cat :query   (s/alt :map     (s/cat :pk         (s/? ::pk)
                                           :conditions ::kv-conditions
                                           :query      (s/? query?))

                           :non-map (s/cat :query (s/? (complement query?))))
           :options (s/? ::options))))

(s/def ::update!-args
  (s/cat :pk            (s/? ::pk)
         :conditions    (s/? map?)
         :kv-conditions ::kv-conditions
         :changes       map?
         :options       (s/? ::options)))

(s/def ::insert!-args
  (s/cat :rows (s/alt :single-row-map    map?
                      :multiple-row-maps (s/coll-of map?)
                      :kv-pairs          ::kv-conditions
                      :columns-rows      (s/cat :columns (s/coll-of keyword?)
                                                :rows    (s/coll-of vector?)))
         :options (s/? ::options)))
