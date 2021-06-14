(ns toucan2.realize
  (:require [potemkin :as p]))

;; TODO -- integrate this into protocols ?

(p/defprotocol+ Realize
  (realize [row]
    "Fully realize either a reducible query, or a result row from that query."))

(extend-protocol Realize
  Object
  (realize [this]
    this)

  ;; Eduction is assumed to be for query results.
  clojure.core.Eduction
  (realize [this]
    (into [] (map realize) this))

  clojure.lang.IReduceInit
  (realize [this]
    (into [] (map realize) this))

  nil
  (realize [_]
    nil))
