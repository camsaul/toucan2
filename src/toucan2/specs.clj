(ns toucan2.specs
  "Specs for things that are used in multiple namespaces."
  (:require [clojure.spec.alpha :as s]))

(s/def ::pk
  (every-pred (complement map?) (complement keyword?)))

(s/def ::kv-conditions
  (s/* (s/cat
        :k keyword?
        :v (complement map?))))

(s/def ::options
  map?)
