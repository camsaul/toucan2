(ns toucan2.realize
  (:require
   [potemkin :as p]
   [toucan2.log :as log]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

;;;; TODO -- this should probably be moved to [[toucan2.protocols]], and be renamed `IRealize` for consistency.
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
    (log/tracef :execute "Already realized: %s" (class this))
    this)

  ;; Eduction is assumed to be for query results.
  ;; TODO -- isn't an Eduction an IReduceInit??
  clojure.core.Eduction
  (realize [this]
    (into [] (map realize) this))

  clojure.lang.IReduceInit
  (realize [this]
    (realize-IReduceInit this))

  nil
  (realize [_]
    nil))
