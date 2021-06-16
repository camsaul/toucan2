(ns toucan.hydrate
  (:require [toucan2.hydrate :as hydrate]
            [potemkin :as p]))

(comment hydrate/keep-me)

(defmacro ^:private import-everything []
  `(p/import-vars
    ~(into ['hydrate]
           (sort (keys (ns-publics 'toucan2.hydrate))))))

(import-everything)

(doseq [[_ varr] (ns-publics *ns*)]
  (alter-meta! varr assoc :deprecated true))
