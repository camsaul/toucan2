(ns bluejdbc.options
  (:require [clojure.data :as data]
            [clojure.tools.logging :as log]
            [methodical.core :as m]
            [potemkin.types :as p.types]))

(p.types/defprotocol+ Options
  "Protocol for anything that has Blue JDBC options, such as the various proxy classes JDBC interfaces."
  (options [this]
    "Get Blue JDBC options map associated with an instance of a Blue JDBC proxy class.")

  (with-options [this new-options]
    "Replace the Blue JDBC options map for an instance of a Blue JDBC proxy class."))

(extend-protocol Options
  Object
  (options [_] nil)

  nil
  (options [_] nil))

(p.types/defprotocol+ CoerceToProperties
  "Protocol for anything that can be coerced to an instance of `java.util.Properties`."
  (->Properties ^java.util.Properties [this]
    "Coerce `this` to a `java.util.Properties`."))

(extend-protocol CoerceToProperties
  nil
  (->Properties [_]
    nil)

  java.util.Properties
  (->Properties [this]
    this)

  clojure.lang.IPersistentMap
  (->Properties [m]
    (let [properties (java.util.Properties.)]
      (doseq [[k v] m]
        (.setProperty properties (name k) (if (keyword? v)
                                            (name v)
                                            (str v))))
      properties)))

(m/defmulti set-option!
  "Apply an option from a map. Should return `jdbc-object` when finished."
  {:arglists '([jdbc-object k v])}
  (fn [jdbc-object k v]
    [(class jdbc-object) (keyword k)]))

(m/defmethod set-option! :before :default
  [jdbc-object k v]
  ;; only log if we're not using the default method
  ;; TODO -- this is an annoying way to have to do this -- https://github.com/camsaul/methodical/ussues/38
  (when (> (count (m/matching-primary-methods set-option! set-option! (m/dispatch-value set-option! jdbc-object k v)))
           1)
    (log/tracef "Set %s option %s -> %s" jdbc-object (pr-str k) (pr-str v)))
  v)

(m/defmethod set-option! :default
  [jdbc-object k v]
  jdbc-object)

;; NOTE! impls of `set-option!` for various classes are in their respective namespaces.

(m/defmulti set-options!
  "Apply options (usually a map) to a JDBC object."
  {:arglists '([jdbc-object options])}
  (fn [jdbc-object options]
    [(class jdbc-object) (class options)]))

(m/defmethod set-options! :default
  [jdbc-object options]
  (throw (ex-info (format "Don't know how to apply options of class %s to %s"
                          (some-> options class .getCanonicalName)
                          (some-> jdbc-object class .getCanonicalName))
                  {})))

(m/defmethod set-options! [:default nil]
  [jdbc-object _]
  jdbc-object)

(m/defmethod set-options! [:default clojure.lang.IPersistentMap]
  [jdbc-object m]
  (let [current-options (options m)]
    (if (= current-options m)
      jdbc-object
      (let [[_ changes] (data/diff current-options m)]
        (reduce
         (fn [jdbc-object [k v]]
           (set-option! jdbc-object k v)
           jdbc-object)
         jdbc-object
         changes)))))

;; TODO -- maybe rename this to `with-options` and `with-options` to `with-options*`?
(defn with-applied-options
  "Changes options of `x` by merging in `new-options` and calls `set-options!` to apply them. If `new-options` are identical to
  existing options, this function is a no-op."
  [x new-options]
  (if (= (options x) new-options)
    x
    (do
      (set-options! x new-options)
      (cond-> (with-options x (merge (options x) new-options))
        (meta x) (with-meta (meta x))))))
