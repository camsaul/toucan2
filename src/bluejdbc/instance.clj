(ns bluejdbc.instance
  #_(:refer-clojure :exclude [instance?])
  (:require [clojure.data :as data]
            [clojure.pprint :as pprint]
            [potemkin :as p]))

(p/defprotocol+ IInstance
  (original [this])
  (changes [this])
  (table [this])
  (with-table [this new-table]))

(p/def-map-type Instance [tbl orig m mta]
  (get [_ k default-value]
    (get m k default-value))
  (assoc [_ k v]
    (Instance. tbl orig (assoc m k v) mta))
  (dissoc [_ k]
    (Instance. tbl orig (dissoc m k) mta))
  (keys [_]
    (keys m))
  (meta [_]
    mta)
  (with-meta [_ new-meta]
    (Instance. tbl orig m new-meta))

  (equiv [_ x]
    (if (satisfies? IInstance x)
      (and (= tbl (table x))
           (= m   x))
      (= m x)))

  IInstance
  (original [_]
    orig)
  (changes [_]
    (second (data/diff orig m)))
  (table [_]
    tbl)
  (with-table [_ new-table]
    (Instance. new-table orig m mta)))

(defmethod print-method Instance
  [^Instance m ^java.io.Writer writer]
  (doseq [^String s ["(bluejdbc.instance/instance "
                     (pr-str (.table m))
                     " "
                     (pr-str (.m m))
                     ")"]]
    (.write writer s)))

(defmethod print-dup Instance
  [m writer]
  (print-method m writer))

(defmethod pprint/simple-dispatch Instance
  [m]
  (print-method m *out*))

(defn instance
  (^bluejdbc.instance.Instance [table]
   (Instance. table {} {} nil))

  (^bluejdbc.instance.Instance [table m]
   (Instance. table m m (meta m)))

  (^bluejdbc.instance.Instance [table k v & more]
   (let [m (into {} (cons [k v] (partition-all 2 more)))]
     (Instance. table m m nil))))

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

  Object
  (table [_]
    nil)
  (original [_]
    nil)
  (changes [_]
    nil)

  clojure.lang.IPersistentMap
  (table [_]
    nil)
  (with-table [m new-table]
    (instance table m))
  (original [_]
    nil)
  (changes [_]
    nil))
