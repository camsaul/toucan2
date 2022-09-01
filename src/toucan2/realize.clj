(ns toucan2.realize
  (:require
   [next.jdbc.result-set :as next.jdbc.rs]
   [potemkin :as p]
   [toucan2.log :as log]
   [toucan2.util :as u])
  (:import
   (next.jdbc.result_set DatafiableRow)))

(set! *warn-on-reflection* true)

(comment next.jdbc.rs/keep-me)

(p/defprotocol+ Realize
  (realize [x]
    "Fully realize either a reducible query, or a result row from that query."))

(defn- realize-IReduceInit [this]
  (log/tracef :results "realize IReduceInit %s" (symbol (.getCanonicalName (class this))))
  (u/try-with-error-context ["realize IReduceInit" {::reducible this}]
    (into []
          (map (fn [row]
                 (log/tracef :results "realize row ^%s %s" (some-> row class .getCanonicalName symbol) row)
                 (u/try-with-error-context ["realize row" {::row row}]
                   (realize row))))
          this)))

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
    (realize-IReduceInit this))

  ;; TODO -- actually pass the real Connection somehow.
  DatafiableRow
  (realize [this]
    (next.jdbc.rs/datafiable-row this nil nil))

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
