(ns toucan2.instance
  (:refer-clojure :exclude [instance?])
  (:require
   [camel-snake-kebab.internals.macros :as csk.macros]
   [clojure.data :as data]
   [methodical.core :as m]
   [next.jdbc.result-set :as jdbc.rset]
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(csk.macros/defconversion "kebab-case" u/lower-case-en u/lower-case-en "-")

(defn default-key-transform [k]
  (when k
    (if (and (keyword? k) (namespace k))
      (keyword (->kebab-case (namespace k)) (->kebab-case (name k)))
      (keyword (->kebab-case (name k))))))

(m/defmulti key-transform-fn
  {:arglists '([model])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod key-transform-fn :default
  [_model]
  ;; TODO -- should this value come from a dynamic variable? You could always implement that behavior yourself if you
  ;; wanted to.
  default-key-transform)

(defn normalize-map [key-xform m]
  {:pre [(fn? key-xform) (map? m)]}
  (into (empty m)
        (map (fn [[k v]]
               [(key-xform k) v]))
        m))

(p/defprotocol+ IInstance
  (original [instance]
    "Get the original version of `instance` as it appeared when it first came out of the DB.")

  (with-original [instance new-original]
    "Return a copy of `instance` with its `original` map set to `new-original`.")

  (current [instance]
    "Return the underlying map representing the current state of an `instance`.")

  (with-current [instance new-current]
    "Return a copy of `instance` with its underlying `current` map set to `new-current`.")

  (changes [instance]
    "Get a map with any changes made to `instance` since it came out of the DB. Only includes keys that have been
    added or given different values; keys that were removed are not counted. Returns `nil` if there are no changes.")

  (model [instance]
    "Get the model associated with `instance`.")

  (with-model [instance new-model]
    "Return a copy of `instance` with its model set to `new-model.`"))

(defn instance?
  "True if `x` is a Toucan2 instance, i.e. a `toucan2.instance.Instance` or some other class that satisfies
  `toucan2.instance.IInstance`."
  [x]
  (clojure.core/instance? toucan2.instance.IInstance x))

;; I know the args are in the opposite order of `instance?`, but it seems like a Yoda Condition to write the code as
;;
;;    (instance-of? ::toucan my-instance)
;;
;; I read that as "::toucan instance-of? my-instance".
;;
;;    (instance-of? my-instance ::toucan)
;;
;; Makes more sense. It also follows the same order as `isa?`.
(defn instance-of?
  "True if `x` is a Toucan2 instance, and its [[model]] `isa?` `a-model`.

    (instance-of? (instance ::toucan {}) ::bird) ; -> true
    (instance-of? (instance ::bird {}) ::toucan) ; -> false"
  [x a-model]
  (and (instance? x)
       (isa? (model x) a-model)))

(declare ->TransientInstance)

(p/def-map-type Instance [mdl ^clojure.lang.IPersistentMap orig ^clojure.lang.IPersistentMap m key-xform mta]
  (get [_ k default-value]
    (get m (key-xform k) default-value))

  (assoc [this k v]
    (let [new-m (assoc m (key-xform k) v)]
      (if (identical? m new-m)
        this
        (Instance. mdl orig new-m key-xform mta))))

  (dissoc [this k]
    (let [new-m (dissoc m (key-xform k))]
      (if (identical? m new-m)
        this
        (Instance. mdl orig new-m key-xform mta))))

  (keys [_]
    (keys m))

  (meta [_]
    mta)

  (with-meta [this new-meta]
    (if (identical? mta new-meta)
      this
      (Instance. mdl orig m key-xform new-meta)))

  clojure.lang.IPersistentCollection
  (equiv [_ x]
    (if (instance? x)
      ;; TODO -- not sure if two instances with different connectables should be considered different. I guess not
      ;; because it makes them inconvenient to use in tests and stuff
      ;;
      ;; TODO -- should a instance be considered equal to a plain map with non-normalized keys? e.g.
      ;;    (= (instance {:updated-at 100}) {:updated_at 100})
      ;;
      ;; I was leaning towards yes but I'm not sure how to make it work in both directions.
      (and #_(= conn (x))
           (= mdl (model x))
           (= m   x))
      (and (map? x)
           (= m (normalize-map key-xform x)))))

  java.util.Map
  (containsKey [_ k]
    (.containsKey m (key-xform k)))

  clojure.lang.IEditableCollection
  (asTransient [_]
    (->TransientInstance mdl (transient m) key-xform mta))

  IInstance
  (original [_]
    orig)

  (with-original [this new-original]
    (if (identical? orig new-original)
      this
      (Instance. mdl new-original m key-xform mta)))

  (current [_]
    m)

  (with-current [this new-current]
    (if (identical? m new-current)
      this
      (Instance. mdl orig new-current key-xform mta)))

  (changes [_]
    (not-empty (second (data/diff orig m))))

  (model [_]
    mdl)

  (with-model [this new-model]
    (if (identical? mdl new-model)
      this
      (Instance. new-model orig m key-xform mta)))

  realize/Realize
  (realize [_]
    (if (identical? orig m)
      (let [m (realize/realize m)]
        (Instance. mdl m m key-xform mta))
      (Instance. mdl (realize/realize orig) (realize/realize m) key-xform mta)))

  pretty/PrettyPrintable
  (pretty [_]
    (list `instance mdl m)))

(deftype ^:private TransientInstance [mdl ^clojure.lang.ITransientMap m key-xform mta]
  clojure.lang.ITransientMap
  (conj [this v]
    (.conj m v)
    this)
  (persistent [_]
    (let [m (persistent! m)]
      (Instance. mdl m m key-xform mta)))
  (assoc [this k v]
    (.assoc m (key-xform k) v)
    this)
  (without [this k]
    (.without m (key-xform k))
    this)

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->TransientInstance mdl m key-xform mta)))

(defn instance
  (^toucan2.instance.Instance [a-model]
   (instance a-model {}))

  (^toucan2.instance.Instance [a-model m]
   (let [key-xform     (key-transform-fn a-model)
         m             (normalize-map key-xform m)]
     (->Instance a-model m m key-xform (meta m))))

  (^toucan2.instance.Instance [a-model k v & more]
   (let [m (into {} (partition-all 2) (list* k v more))]
     (instance a-model m))))

(extend-protocol IInstance
  nil
  (original [_]
    nil)
  (current [_]
    nil)
  (changes [_]
    nil)
  (model [_]
    nil)
  (with-model [_ new-model]
    (instance new-model))

  ;; TODO -- not sure what the correct behavior for objects should be... if I call (original 1) I think that should
  ;; throw an Exception rather than returning nil, because that makes no sense.
  ;;
  Object
  (model [_]
    nil)

  ;; generally just treat a plain map like an instance with nil model/and original = nil,
  ;; and no-op for anything that would require "upgrading" the map to an actual instance in such a way that if
  ;;
  ;;    (= plain-map instance)
  ;;
  ;; then
  ;;
  ;;    (= (f plain-map) (f instance))
  clojure.lang.IPersistentMap
  (model [_]
    nil)
  (with-model [m a-model]
    (instance a-model m))
  (original [_]
    nil)
  (current [m]
    m)
  (with-current [_ new-current]
    new-current)
  (with-original [m _]
    m)
  (changes [_]
    nil))

(defn reset-original
  "Return a copy of `instance` with its `original` value set to its current value, discarding the previous original
  value. No-ops if `instance` is not a Toucan 2 instance."
  [instance]
  (if (instance? instance)
    (with-original instance (current instance))
    instance))

;; TODO -- should we have a revert-changes helper function as well?

(defn update-original
  "Applies `f` directly to the underlying `original` map of an `instance`. No-ops if `instance` is not an `Instance`."
  [instance f & args]
  (if (instance? instance)
    (with-original instance (apply f (original instance) args))
    instance))

(defn update-current
  "Applies `f` directly to the underlying `current` map of an `instance`; useful if you need to operate on it directly.
  Acts like regular `(apply f instance args)` if `instance` is not an `Instance`."
  [instance f & args]
  (with-current instance (apply f (current instance) args)))

(defn update-original-and-current
  "Like `(apply f instance args)`, but affects both the `original` map and `current` map of `instance` rather than just
  the current map. Acts like regular `(apply f instance args)` if `instance` is not an `Instance`.

  `f` is applied directly to the underlying `original` and `current` maps of `instance` itself. `f` is only applied
  once if `original` and `current` are currently the same object (i.e., the new `original` and `current` will also be
  the same object). If `current` and `original` are not the same object, `f` is applied twice."
  [instance f & args]
  (if (identical? (original instance) (current instance))
    (reset-original (apply update-current instance f args))
    (as-> instance instance
      (apply update-original instance f args)
      (apply update-current  instance f args))))

(p/defrecord+ InstanceResultSetBuilder [mdl key-xform ^java.sql.ResultSet rset rsmeta cols]
  jdbc.rset/RowBuilder
  (->row [_this]
    (->TransientInstance mdl (transient {}) key-xform nil))
  (column-count [_this]
    (count cols))
  (with-column [this row i]
    (jdbc.rset/with-column-value this row (nth cols (dec i))
      (jdbc.rset/read-column-by-index (.getObject rset ^Integer i) rsmeta i)))
  (with-column-value [_this row col v]
    (assoc! row col v))
  (row! [_this row]
    (persistent! row))

  ;; this is actually not used by [[next.jdbc/plan]] apparently
  jdbc.rset/ResultSetBuilder
  (->rs [_this]
    (transient []))
  (with-row [_this mrs row]
    (conj! mrs row))
  (rs! [_this mrs]
    (persistent! mrs)))

(defn instance-result-set-builder [a-model]
  (let [key-xform (key-transform-fn a-model)]
    (fn [^java.sql.ResultSet rset _opts]
      (let [rsmeta (.getMetaData rset)
            cols   (mapv (fn [^Integer i]
                           (key-xform (.getColumnLabel rsmeta i)))
                         (range 1 (inc (.getColumnCount rsmeta))))]
        (->InstanceResultSetBuilder a-model key-xform rset rsmeta cols)))))
