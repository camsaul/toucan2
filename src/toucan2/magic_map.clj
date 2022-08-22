(ns toucan2.magic-map
  "A map type that automatically applies key transforms to keys when you create the map, add values to it, and when you
  fetch values from it."
  (:require
   [camel-snake-kebab.internals.macros :as csk.macros]
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan2.util :as u]))

(csk.macros/defconversion "kebab-case" u/lower-case-en u/lower-case-en "-")

(defn kebab-case-xform
  "The default magic map transform function."
  [k]
  (when k
    (if (and (clojure.core/instance? clojure.lang.Named k) (namespace k))
      (keyword (->kebab-case (namespace k)) (->kebab-case (name k)))
      (keyword (->kebab-case (name k))))))

(def ^:dynamic *key-transform-fn* #'kebab-case-xform)

(defn normalize-map [key-xform m]
  {:pre [(ifn? key-xform) (map? m)]}
  (into (empty m)
        (map (fn [[k v]]
               [(key-xform k) v]))
        m))

(p/def-map-type MagicMap [^clojure.lang.IPersistentMap m key-xform mta]
  (get [_ k default-value]
    (get m (key-xform k) default-value))

  (assoc [this k v]
    (let [new-m (assoc m (key-xform k) v)]
      (if (identical? m new-m)
        this
        (MagicMap. new-m key-xform mta))))

  (dissoc [this k]
    (let [new-m (dissoc m (key-xform k))]
      (if (identical? m new-m)
        this
        (MagicMap. new-m key-xform mta))))

  (keys [_this]
    (keys m))

  (meta [_this]
    mta)

  (with-meta [this new-meta]
    (if (identical? mta new-meta)
      this
      (MagicMap. m key-xform new-meta)))

  clojure.lang.IPersistentCollection
  (equiv [this x]
    (and (map? x)
         (if (instance? (class this) x)
           (= m x)
           ;; a plain non-Magic map should be equal to this if it's equal to us when you normalize it
           (= m (normalize-map key-xform x)))))

  java.util.Map
  (containsKey [_ k]
    (.containsKey m (key-xform k)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `magic-map
          (if (instance? pretty.core.PrettyPrintable m)
            (pretty/pretty m)
            m)
          key-xform)))

(defn magic-map
  ([]
   (magic-map {}))
  ([m]
   (magic-map m *key-transform-fn*))
  ([m key-xform]
   (magic-map m key-xform (meta m)))
  ([m key-xform metta]
   {:pre [(ifn? key-xform)]}
   (->MagicMap (normalize-map key-xform m)
               key-xform
               metta)))
