(ns bluejdbc.instance
  (:require [clojure.data :as data]
            [clojure.pprint :as pprint]
            [potemkin :as p]))

(p/defprotocol+ IInstance
  (original [this])
  (changes [this])
  (table [this])
  (with-table [this new-table])
  (connectable [this])
  (with-connectable [this new-connectable]))

(declare ->TransientInstance)

(p/def-map-type Instance [conn tbl orig m mta]
  (get [_ k default-value]
    (get m k default-value))
  (assoc [_ k v]
    (Instance. conn tbl orig (assoc m k v) mta))
  (dissoc [_ k]
    (Instance. conn tbl orig (dissoc m k) mta))
  (keys [_]
    (keys m))
  (meta [_]
    mta)
  (with-meta [_ new-meta]
    (Instance. conn tbl orig m new-meta))

  (equiv [_ x]
    (if (instance? Instance x)
      ;; TODO -- not sure if two instances with different connectables should be considered different. I guess not
      ;; because it makes them inconvenient to use in tests and stuff
      (and #_(= conn (connectable x))
           (= tbl (table x))
           (= m   x))
      (= m x)))

  clojure.lang.IEditableCollection
  (asTransient [_]
    (->TransientInstance conn tbl (transient m) mta))

  IInstance
  (original [_]
    orig)
  (changes [_]
    (second (data/diff orig m)))
  (table [_]
    tbl)
  (with-table [_ new-table]
    (Instance. conn new-table orig m mta))
  (connectable [_]
    conn)
  (with-connectable [_ new-connectable]
    (Instance. new-connectable tbl orig m mta)))

(deftype ^:private TransientInstance [conn tbl ^clojure.lang.ITransientMap m mta]
  clojure.lang.ITransientMap
  (conj [this v]
    (.conj m v)
    this)
  (persistent [_]
    (let [m (persistent! m)]
      (Instance. conn tbl m m mta)))
  (assoc [this k v]
    (.assoc m k v)
    this)
  (without [this k]
    (.without m k)
    this))

;; TODO -- why don't we use `pretty/PrettyPrintable` here?
(defmethod print-method Instance
  [^Instance m ^java.io.Writer writer]
  (binding [*out* writer]
    (pr (list `instance (.connectable m) (.table m) (.m m)))))

(defmethod print-dup Instance
  [m writer]
  (print-method m writer))

(defmethod pprint/simple-dispatch Instance
  [m]
  (print-method m *out*))

(defn instance
  (^bluejdbc.instance.Instance [table]
   ;; TODO -- shouldn't this default to `conn/*connectable*` ??
   (Instance. nil table {} {} nil))

  (^bluejdbc.instance.Instance [table m]
   (Instance. nil table m m (meta m)))

  (^bluejdbc.instance.Instance [connectable table m]
   (Instance. connectable table m m (meta m)))

  (^bluejdbc.instance.Instance [connectable table k v & more]
   (let [m (into {} (cons [k v] (partition-all 2 more)))]
     (Instance. connectable table m m nil))))

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
