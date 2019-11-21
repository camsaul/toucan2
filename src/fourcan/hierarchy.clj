(ns fourcan.hierarchy
  (:refer-clojure :exclude [derive]))

(defonce hierarchy (make-hierarchy))

(defn derive [tag parent]
  (alter-var-root #'hierarchy clojure.core/derive tag parent))
