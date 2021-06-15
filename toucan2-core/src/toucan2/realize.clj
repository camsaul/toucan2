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
  ;; TODO -- isn't an Eduction an IReduceInit??
  clojure.core.Eduction
  (realize [this]
    (into [] (map realize) this))

  clojure.lang.IReduceInit
  (realize [this]
    (try
      (into [] (map realize) this)
      (catch Throwable e
        (throw (ex-info (format "Error realizing IReduceInit %s: %s"
                                (binding [*print-meta* true] (pr-str this))
                                (ex-message e))
                        {:this this}
                        e)))))

  nil
  (realize [_]
    nil))
