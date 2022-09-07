(ns toucan2.magic-map
  "A map type that automatically applies key transforms to keys when you create the map, add values to it, and when you
  fetch values from it."
  (:require
   [camel-snake-kebab.internals.macros :as csk.macros]
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

;;; These declarations are to stop Kondo from complaining. See
;;; https://github.com/clj-kondo/clj-kondo/blob/master/doc/linters.md#unresolved-symbol
(declare ->kebab-case)
(declare ->kebab-case-keyword)
(declare ->kebab-case-string)
(declare ->kebab-case-symbol)

(csk.macros/defconversion "kebab-case" u/lower-case-en u/lower-case-en "-")

(defonce print-magic-maps (atom true))
(def ^:dynamic *print-magic-maps* nil)

(defn- print-magic-maps? []
  (if (some? *print-magic-maps*)
    *print-magic-maps*
    @print-magic-maps))

(defn kebab-case-xform
  "The default magic map transform function. Converts things to `kebab-case`, preserving namespaces."
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

(declare ->TransientMagicMap)

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

  clojure.lang.IEditableCollection
  (asTransient [_this]
    (->TransientMagicMap (transient m) key-xform mta))

  pretty/PrettyPrintable
  (pretty [_this]
    (if-not (print-magic-maps?)
      m
      (list `magic-map
            (cond-> m
              (instance? pretty.core.PrettyPrintable m)
              pretty/pretty)
            key-xform))))

(deftype ^:no-doc TransientMagicMap [^clojure.lang.ITransientMap m key-xform mta]
  clojure.lang.ITransientMap
  (conj [this [k v]]
    (.conj m [(key-xform k) v])
    this)
  (persistent [_this]
    (let [m (persistent! m)]
      (MagicMap. m key-xform mta)))
  (assoc [this k v]
    (.assoc m (key-xform k) v)
    this)
  (without [this k]
    (.without m (key-xform k))
    this)
  (valAt [_this k]
    (.valAt m k))
  (valAt [_this k not-found]
    (.valAt m k not-found))
  (count [_this]
    (count m))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->TransientMagicMap m key-xform mta)))

(defn magic-map
  (^toucan2.magic_map.MagicMap []
   (magic-map {}))
  (^toucan2.magic_map.MagicMap [m]
   (magic-map m *key-transform-fn*))
  (^toucan2.magic_map.MagicMap [m key-xform]
   (magic-map m key-xform (meta m)))
  (^toucan2.magic_map.MagicMap [m key-xform metta]
   {:pre [(ifn? key-xform)]}
   (->MagicMap (normalize-map key-xform m)
               key-xform
               metta)))

(defn magic-map? [m]
  (instance? toucan2.magic_map.MagicMap m))
