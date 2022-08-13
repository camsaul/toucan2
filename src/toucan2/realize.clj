(ns toucan2.realize
  (:require [potemkin :as p]
            [toucan2.util :as u]
            [next.jdbc.result-set :as jdbc.rset])
  (:import next.jdbc.result_set.DatafiableRow))

(comment jdbc.rset/keep-me)

(p/defprotocol+ Realize
  (realize [x]
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
      (u/with-debug-result (format "(%s ^%s %s)" `realize (.getCanonicalName (class this)) (pr-str this))
        (into []
              (map (fn [row]
                     (u/with-debug-result (format "(%s ^%s %s)" `realize (some-> row class .getCanonicalName) (pr-str row))
                       (realize row))))
              this))
      (catch Throwable e
        (throw (ex-info (format "Error reducing ^%s %s: %s"
                                (.getCanonicalName (class this))
                                (binding [*print-meta* true] (pr-str this))
                                (ex-message e))
                        {:this this}
                        e)))))

  ;; TODO -- actually pass the real Connection somehow.
  DatafiableRow
  (realize [this]
    (jdbc.rset/datafiable-row this nil nil))

  nil
  (realize [_]
    nil))

(defn reduce-first
  ([reducible]
   (reduce-first (map realize) reducible))

  ([xform reducible]
   (transduce
    (comp xform (take 1))
    (completing conj (comp first))
    []
    reducible)))
