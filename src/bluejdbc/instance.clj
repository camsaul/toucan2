(ns bluejdbc.instance
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.util :as u]
            [camel-snake-kebab.core :as csk]
            [clojure.data :as data]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]))

(defn normalize-key [k]
  (when k
    (if (and (keyword? k) (namespace k))
      (keyword (csk/->kebab-case (namespace k)) (csk/->kebab-case (name k)))
      (keyword (csk/->kebab-case k)))))

(m/defmulti key-transform-fn*
  {:arglists '([connectable tableable])}
  u/dispatch-on-first-two-args)

(m/defmethod key-transform-fn* :default
  [_ _]
  ;; TODO -- should this value come from a dynamic variable? You could always implement that behavior yourself if you
  ;; wanted to.
  normalize-key)

(p/defprotocol+ IInstance
  (original [instance]
    "Get the original version of `instance` as it appeared when it first came out of the DB.")

  (changes [instance]
    "Get a map with any changes made to `instance` since it came out of the DB.")

  ;; TODO -- these should probably be renamed to `tableable` and `with-tableable` respectively.
  (table [instance]
    ("Get the tableable associated with `instance`."))

  (with-table [instance new-tableable]
    "Return a copy of `instance` with its tableable set to `new-tableable.`")

  (connectable [instance]
    "Get the connectable associated with `instance`.")

  (with-connectable [instance new-connectable]
    "Return a copy of `instance` with its connectable set to `new-connectable.`"))

(declare ->TransientInstance)

(p/def-map-type Instance [conn tbl orig m key-xform mta]
  (get [_ k default-value]
    (get m (key-xform k) default-value))
  (assoc [_ k v]
    (Instance. conn tbl orig (assoc m (key-xform k) v) key-xform mta))
  (dissoc [_ k]
    (Instance. conn tbl orig (dissoc m (key-xform k)) key-xform mta))
  (keys [_]
    (keys m))
  (meta [_]
    mta)
  (with-meta [_ new-meta]
    (Instance. conn tbl orig m key-xform new-meta))

  (equiv [_ x]
    (if (instance? Instance x)
      ;; TODO -- not sure if two instances with different connectables should be considered different. I guess not
      ;; because it makes them inconvenient to use in tests and stuff
      ;;
      ;; TODO -- should a instance be considered equal to a plain map with non-normalized keys? e.g.
      ;;    (= (instance {:updated-at 100}) {:updated_at 100})
      ;;
      ;; I was leaning towards yes but I'm not sure how to make it work in both directions.
      (and #_(= conn (connectable x))
           (= tbl (table x))
           (= m   x))
      (= m x)))

  clojure.lang.IEditableCollection
  (asTransient [_]
    (->TransientInstance conn tbl (transient m) key-xform mta))

  IInstance
  (original [_]
    orig)
  (changes [_]
    (second (data/diff orig m)))
  (table [_]
    tbl)
  (with-table [_ new-table]
    (Instance. conn new-table orig m key-xform mta))
  (connectable [_]
    conn)
  (with-connectable [_ new-connectable]
    (Instance. new-connectable tbl orig m key-xform mta))

  pretty/PrettyPrintable
  (pretty [_]
    (list (u/qualify-symbol-for-*ns* `instance) conn tbl m)))

(deftype ^:private TransientInstance [conn tbl ^clojure.lang.ITransientMap m key-xform mta]
  clojure.lang.ITransientMap
  (conj [this v]
    (.conj m v)
    this)
  (persistent [_]
    (let [m (persistent! m)]
      (Instance. conn tbl m m key-xform mta)))
  (assoc [this k v]
    (.assoc m (key-xform k) v)
    this)
  (without [this k]
    (.without m (key-xform k))
    this))

(defn normalize-map [key-xform m]
  (into (empty m)
        (map (fn [[k v]]
               [(key-xform k) v]))
        m))

(defn instance
  (^bluejdbc.instance.Instance [tableable]
   (instance conn.current/*current-connectable* tableable {}))

  (^bluejdbc.instance.Instance [tableable m]
   (instance conn.current/*current-connectable* tableable m))

  (^bluejdbc.instance.Instance [connectable tableable m]
   (let [key-xform (key-transform-fn* connectable tableable)
         m         (normalize-map key-xform m)]
     (Instance. connectable tableable m m key-xform (meta m))))

  (^bluejdbc.instance.Instance [connectable tableable k v & more]
   (let [m (into {} (partition-all 2) (list* k v more))]
     (instance connectable tableable m))))

(extend-protocol IInstance
  nil
  (table [_]
    nil)
  (original [_]
    nil)
  (changes [_]
    nil)
  (with-table [_ new-table]
    (instance new-table))
  (connectable [_]
    nil)
  (with-connectable [this _]
    (instance connectable nil nil))

  Object
  (table [_]
    nil)
  (original [_]
    nil)
  (changes [_]
    nil)
  (connectable [_]
    nil)

  clojure.lang.IPersistentMap
  (table [_]
    nil)
  (with-table [m new-table]
    (instance table m))
  (original [_]
    nil)
  (changes [_]
    nil)
  (connectable [_]
    nil)
  (with-connectable [_ m]
    (instance connectable nil m)))
