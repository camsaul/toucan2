(ns bluejdbc.options
  (:require [methodical.core :as m]
            [potemkin.types :as p.types]))

(p.types/defprotocol+ CoerceToProperties
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

(m/defmethod set-option! :default
  [jdbc-object k v]
  jdbc-object)

(m/defmulti set-options!
  "Apply options (usually a map) to a JDBC object."
  {:arglists '([jdbc-object options])}
  (fn [jdbc-object options]
    [(class jdbc-object) (class options)]))

(m/defmethod set-options! :default
  [jdbc-object options]
  ;; TODO -- need newer version of Methodical to fix this
  (cond
    (nil? options)
    jdbc-object

    (map? options)
    (reduce
     (fn [jdbc-object [k v]]
       (set-option! jdbc-object k v))
     jdbc-object
     options)

    :else
    (throw (ex-info (format "Don't know how to apply options of class %s to %s"
                            (some-> options class .getCanonicalName)
                            (some-> jdbc-object class .getCanonicalName))
                    {}))))

;; TODO -- fix me
#_
(m/defmethod set-options! [:default nil]
  [jdbc-object _]
  jdbc-object)

#_(m/defmethod set-options! [:default clojure.lang.IPersistentMap]
  [jdbc-object m]
  (reduce
   (fn [jdbc-object [k v]]
     (set-option! jdbc-object k v))
   jdbc-object
   m))

;; impls of `set-option!` for various classes are in their respective namespaces.
