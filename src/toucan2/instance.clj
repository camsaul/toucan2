(ns toucan2.instance
  (:refer-clojure :exclude [instance?])
  (:require
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]))

(set! *warn-on-reflection* true)

(defn instance?
  "True if `x` is a Toucan2 instance, i.e. a `toucan2.instance.Instance` or some other class that satisfies the correct
  interfaces.

  Toucan instances need to implement [[protocols/IModel]], [[protocols/IWithModel]], and [[protocols/IRecordChanges]]."
  [x]
  (every? #(clojure.core/instance? % x)
          [toucan2.protocols.IModel
           toucan2.protocols.IWithModel
           toucan2.protocols.IRecordChanges]))

(defn instance-of?
  "True if `x` is a Toucan2 instance, and its [[protocols/model]] `isa?` `model`.

  ```clj
  (instance-of? ::bird (instance ::toucan {})) ; -> true
  (instance-of?  ::toucan (instance ::bird {})) ; -> false
  ```"
  [model x]
  (and (instance? x)
       (isa? (protocols/model x) model)))

(defn- changes*
  "Changes between `orig` and `m`. Unlike [[clojure.data/diff]], this is a shallow diff. Only columns that are added or
  modified should be returned."
  [original current]
  (if (or (not (map? original))
          (not (map? current)))
    current
    (not-empty
     (into
      (empty original)
      (map (fn [k]
             (let [original-value (get original k)
                   current-value  (get current k)]
               (when-not (= original-value current-value)
                 [k current-value]))))
      (keys current)))))

(declare ->TransientInstance)

(p/def-map-type Instance [model
                          ^clojure.lang.IPersistentMap orig
                          ^clojure.lang.IPersistentMap m
                          mta]
  (get [_ k default-value]
    (get m k default-value))

  (assoc [this k v]
    (let [m' (assoc m k v)]
      (if (identical? m m')
        this
        (Instance. model orig m' mta))))

  (dissoc [this k]
    (let [m' (dissoc m k)]
      (if (identical? m m')
        this
        (Instance. model orig m' mta))))

  (keys [_this]
    (keys m))

  (meta [_this]
    mta)

  (with-meta [this new-meta]
    (if (identical? mta new-meta)
      this
      (Instance. model orig m new-meta)))

  clojure.lang.IPersistentCollection
  (cons [this o]
    (cond
      (map? o)
      (reduce #(apply assoc %1 %2) this o)

      (clojure.core/instance? java.util.Map o)
      (reduce
       #(apply assoc %1 %2)
       this
       (into {} o))

      :else
      (if-let [[k v] (seq o)]
        (assoc this k v)
        this)))

  (equiv [_this another]
    (cond
      (clojure.core/instance? toucan2.protocols.IModel another)
      (and (= model (protocols/model another))
           (= m another))

      (map? another)
      (= m another)

      :else
      false))

  (empty [_this]
    (Instance. model (empty orig) (empty m) mta))

  java.util.Map
  (containsKey [_this k]
    (.containsKey m k))

  clojure.lang.IEditableCollection
  (asTransient [_this]
    (->TransientInstance model (transient m) mta))

  protocols/IModel
  (protocols/model [_this]
    model)

  protocols/IWithModel
  (with-model [this new-model]
    (if (= model new-model)
      this
      (Instance. new-model orig m mta)))

  protocols/IRecordChanges
  (original [_this]
    orig)

  (with-original [this new-original]
    (if (identical? orig new-original)
      this
      (let [new-original (if (nil? new-original)
                           {}
                           new-original)]
        (assert (map? new-original))
        (Instance. model new-original m mta))))

  (current [_this]
    m)

  (with-current [this new-current]
    (if (identical? m new-current)
      this
      (let [new-current (if (nil? new-current)
                          {}
                          new-current)]
        (assert (map? new-current))
        (Instance. model orig new-current mta))))

  (changes [_this]
    (not-empty (changes* orig m)))

  protocols/IDispatchValue
  (dispatch-value [_this]
    (protocols/dispatch-value model))

  realize/Realize
  (realize [_this]
    (if (identical? orig m)
      (let [m (realize/realize m)]
        (Instance. model m m mta))
      (Instance. model (realize/realize orig) (realize/realize m) mta)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `instance model m)))

(deftype ^:no-doc TransientInstance [model ^clojure.lang.ITransientMap m mta]
  clojure.lang.ITransientMap
  (conj [this v]
    (let [m' (conj! m v)]
      (if (identical? m m')
        this
        (TransientInstance. model m' mta))))

  (persistent [_this]
    (let [m (persistent! m)]
      (Instance. model m m mta)))

  (assoc [this k v]
    (let [m' (assoc! m k v)]
      (if (identical? m m')
        this
        (TransientInstance. model m' mta))))

  (without [this k]
    (let [m' (dissoc! m k)]
      (if (identical? m m')
        this
        (TransientInstance. model m' mta))))

  (valAt [_this k]
    (.valAt m k))

  (valAt [_this k not-found]
    (.valAt m k not-found))

  (count [_this]
    (count m))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->TransientInstance model m mta)))

(defn instance
  (^toucan2.instance.Instance []
   (instance nil))

  (^toucan2.instance.Instance [model]
   (instance model {}))

  (^toucan2.instance.Instance [model m]
   {:pre [((some-fn map? nil?) m)]}
   (cond
     ;; optimization: if `m` is already an instance with `model` return it as-is.
     (and (instance? m)
          (= (protocols/model m) model))
     m

     ;; DISABLED FOR NOW BECAUSE MAYBE THE OTHER MODEL HAS A DIFFERENT UNDERLYING EMPTY MAP OR KEY TRANSFORM
     ;;
     ;; ;; optimization 2: if `m` is an instance of something else use [[protocols/with-model]]
     ;; (instance? m)
     ;; (protocols/with-model m model)

     :else
     (let [m* (into {} m)]
       (->Instance model m* m* (meta m)))))

  (^toucan2.instance.Instance [model k v & more]
   (let [m (into {} (partition-all 2) (list* k v more))]
     (instance model m))))

(extend-protocol protocols/IWithModel
  nil
  (with-model [_this model]
    (instance model))

  clojure.lang.IPersistentMap
  (with-model [m model]
    (instance model m)))

(defn reset-original
  "Return a copy of `an-instance` with its `original` value set to its current value, discarding the previous original
  value. No-ops if `an-instance` is not a Toucan 2 instance."
  [an-instance]
  (if (instance? an-instance)
    (protocols/with-original an-instance (protocols/current an-instance))
    an-instance))

;;; TODO -- should we have a revert-changes helper function as well?

(defn update-original
  "Applies `f` directly to the underlying `original` map of `an-instance`. No-ops if `an-instance` is not
  an [[Instance]]."
  [an-instance f & args]
  (if (instance? an-instance)
    (protocols/with-original an-instance (apply f (protocols/original an-instance) args))
    an-instance))

(defn update-current
  "Applies `f` directly to the underlying `current` map of `an-instance`; useful if you need to operate on it directly.
  Acts like regular `(apply f instance args)` if `an-instance` is not an [[Instance]]."
  [an-instance f & args]
  (protocols/with-current an-instance (apply f (protocols/current an-instance) args)))

(defn update-original-and-current
  "Like `(apply f instance args)`, but affects both the `original` map and `current` map of `an-instance` rather than just
  the current map. Acts like regular `(apply f instance args)` if `instance` is not an `Instance`.

  `f` is applied directly to the underlying `original` and `current` maps of `instance` itself. `f` is only applied
  once if `original` and `current` are currently the same object (i.e., the new `original` and `current` will also be
  the same object). If `current` and `original` are not the same object, `f` is applied twice."
  [an-instance f & args]
  (if (identical? (protocols/original an-instance) (protocols/current an-instance))
    (reset-original (apply update-current an-instance f args))
    (as-> an-instance an-instance
      (apply update-original an-instance f args)
      (apply update-current  an-instance f args))))
