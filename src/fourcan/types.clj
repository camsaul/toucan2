(ns fourcan.types
  (:refer-clojure :exclude [instance?])
  (:require [clojure.pprint :as pprint]
            [fourcan.hierarchy :as hierarchy]
            [potemkin.collections :as p.collections]
            [methodical.core :as m]
            [potemkin.types :as p.types]
            [clojure.data :as data]
            [pretty.core :as pretty]))

(doseq [method [print-method pprint/simple-dispatch]]
  (prefer-method method pretty.core.PrettyPrintable clojure.lang.ISeq))

(p.types/defprotocol+ Model
  (model [this]))

(defn dispatch-on-model
  ([x]           (model x))
  ([x _]         (model x))
  ([x _ _]       (model x))
  ([x _ _ _]     (model x))
  ([x _ _ _ & _] (model x)))

(extend-protocol Model
  Object
  (model [_] nil)

  nil
  (model [_] nil)

  clojure.lang.Keyword
  (model [this] this)

  clojure.lang.Symbol
  (model [this] (keyword this)))

(m/defmulti invoke-query
  {:arglists '([query])}
  dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(declare honeysql-form)

(p.types/deftype+ Query [modl hsql options mta]
  Model
  (model [_]
    modl)

  clojure.lang.IMeta
  (meta [_] mta)

  clojure.lang.IObj
  (withMeta [_ new-meta]
    (Query. modl hsql options new-meta))

  pretty/PrettyPrintable
  (pretty [_]
    (concat
     (list 'query)
     (when modl
       (list modl))
     (when (seq hsql)
       (list hsql))
     (when (seq options)
       (list options))))

  ;; TODO - these should probably throw an Exception if Query is SQL
  clojure.lang.IPersistentMap
  (assoc [_ k v]
    (Query. modl (assoc hsql k v) options mta))
  (assocEx [_ k v]
    (Query. modl (.assocEx hsql k v) options mta))
  (without [_ k]
    (Query. modl (.without hsql k) options mta))

  clojure.lang.Associative
  (containsKey [_ k]
    (contains? hsql k))
  (entryAt [_ k]
    (.entryAt hsql k))

  clojure.lang.IPersistentCollection
  (count [this]
    (Query. modl (assoc hsql :select [[:%count.* :count]]) options mta))
  ;; TODO - consider whether this makes sense
  (empty [_]
    (Query. modl (empty hsql) options mta))
  (equiv [_ another]
    (and (isa? (class another) Query)
         (= modl (model another))
         (= hsql (honeysql-form another))))

  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt hsql k))
  (valAt [_ k not-found]
    (.valAt hsql k not-found))

  clojure.lang.IFn
  (applyTo [_ arglist]
    (apply hsql arglist))
  (invoke [this]
    (invoke-query this))

  java.lang.Iterable
  (iterator [this]
    (clojure.lang.SeqIterator. (this)))

  clojure.lang.Seqable
  (seq [this]
    (seq (this)))

  clojure.lang.ISeq
  (first [this]
    (first (this)))
  (next [this]
    (next (this)))
  (more [this]
    (rest (this)))
  (cons [this obj]
    (Query. modl (conj hsql obj) options mta)))

(defn query? [x]
  (clojure.core/instance? Query x))

(defn honeysql-form [^Query query]
  (when query
    (.hsql query)))

(defn query-options [^Query query]
  (when query
    (.options query)))

(p.types/deftype+ Instance [modl orig m mta]
  Model
  (model [_]
    modl)

  p.collections/AbstractMap
  (get*       [_ k default-value] (get m k default-value))
  (assoc*     [_ k v]             (Instance. modl orig (assoc m k v) mta))
  (dissoc*    [_ k]               (Instance. modl orig (dissoc m k) mta))
  (keys*      [_]                 (keys m))
  (meta*      [_]                 mta)
  (with-meta* [_ new-meta]        (Instance. modl orig m new-meta))

  clojure.lang.IPersistentCollection
  (empty [_]
    (Instance. modl (empty orig) (empty m) mta))

  pretty/PrettyPrintable
  (pretty [_]
    ;; TODO - `toucan.db/instance-of` (?)
    (if (seq m)
      (list 'instance modl m)
      (list 'instance modl)))

  ;; clojure.lang.IFn
  ;; (applyTo [this arglist]
  ;;   (apply m arglist))
  ;; (invoke [_ k]
  ;;   (get m k))
  ;; (invoke [_ k not-found]
  ;;   (get m k not-found))
  )

(defn instance? [x]
  (clojure.core/instance? Instance x))

(defn original [^Instance m]
  (when (clojure.core/instance? Instance m)
    (.orig m)))

(defn instance
  (^Instance [a-model]
   (instance a-model {} {} nil))

  (^Instance [a-model m]
   (instance a-model m m nil))

  (^Instance [a-model orig m]
   (instance a-model orig m nil))

  (^Instance [a-model orig m mta]
   (Instance. (model a-model) orig m mta)))

;; TODO - dox
(defn of
  {:style/indent 1}
  (^fourcan.types.Instance [model]
   (instance model nil nil nil))

  ;; TODO - not 100% sure calling `model` here makes sense... what if we do something like the following (see below)
  (^fourcan.types.Instance [model m]
   (assert ((some-fn nil? map?) m)
           (format "Not a map: %s" m))
   (instance model m m (meta m))))

(defn changes [m]
  (when (seq m)
    (second (data/diff (original m) m))))
