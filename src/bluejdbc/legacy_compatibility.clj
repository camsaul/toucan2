(ns bluejdbc.legacy-compatibility
  "Legacy compatibility for migrating from Toucan."
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.helpers :as helpers]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [honeysql.format :as hformat]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]
            [toucan.models :as t.models]))

(defn- toucan-map [model-name table-name]
  {:table               (keyword table-name)
   :name                model-name
   :toucan.models/model true})

(p/deftype+ ^:deprecated LegacyModel [model-name tbl ^clojure.lang.IPersistentMap m]
  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt m k))
  (valAt [_ k default]
    (.valAt m k default))

  clojure.lang.IPersistentCollection
  (count [_]
    (count m))
  (cons [_ x]
    (LegacyModel. model-name tbl (.cons m x)))
  (empty [_]
    (LegacyModel. model-name tbl (empty m)))
  (equiv [_ another]
    (.equiv m another))

  clojure.lang.Associative
  (containsKey [_ k]
    (.containsKey m k))
  (entryAt [_ k]
    (.entryAt m k))
  (assoc [_ k v]
    (LegacyModel. model-name tbl (.assoc m k v)))
  (seq [_]
    (seq m))

  clojure.lang.IPersistentMap
  (assocEx [_ k v]
    (LegacyModel. model-name tbl (.assocEx m k v)))
  (without [_ k]
    (LegacyModel. model-name tbl (.without m k)))

  Iterable
  (forEach [_ action]
    (.forEach m action))
  (iterator [_]
    (.iterator m))
  (spliterator [_]
    (.spliterator m))

  clojure.lang.IFn
  (invoke [_]                                              (select/select tbl))
  (invoke [_ pk]                                           (select/select-one tbl pk))
  (invoke [_ a b]                                          (select/select-one tbl a b))
  (invoke [_ a b c]                                        (select/select-one tbl a b c))
  (invoke [_ a b c d]                                      (select/select-one tbl a b c d))
  (invoke [_ a b c d e]                                    (select/select-one tbl a b c d e))
  (invoke [_ a b c d e f]                                  (select/select-one tbl a b c d e f))
  (invoke [_ a b c d e f g]                                (select/select-one tbl a b c d e f g))
  (invoke [_ a b c d e f g h]                              (select/select-one tbl a b c d e f g h))
  (invoke [_ a b c d e f g h i]                            (select/select-one tbl a b c d e f g h i))
  (invoke [_ a b c d e f g h i j]                          (select/select-one tbl a b c d e f g h i j))
  (invoke [_ a b c d e f g h i j k]                        (select/select-one tbl a b c d e f g h i j k))
  (invoke [_ a b c d e f g h i j k l]                      (select/select-one tbl a b c d e f g h i j k l))
  (invoke [_ a b c d e f g h i j k l m]                    (select/select-one tbl a b c d e f g h i j k l m))
  (invoke [_ a b c d e f g h i j k l m n]                  (select/select-one tbl a b c d e f g h i j k l m n))
  (invoke [_ a b c d e f g h i j k l m n o]                (select/select-one tbl a b c d e f g h i j k l m n o))
  (invoke [_ a b c d e f g h i j k l m n o p]              (select/select-one tbl a b c d e f g h i j k l m n o p))
  (invoke [_ a b c d e f g h i j k l m n o p q]            (select/select-one tbl a b c d e f g h i j k l m n o p q))
  (invoke [_ a b c d e f g h i j k l m n o p q r]          (select/select-one tbl a b c d e f g h i j k l m n o p q r))
  (invoke [_ a b c d e f g h i j k l m n o p q r s]        (select/select-one tbl a b c d e f g h i j k l m n o p q r s))
  (invoke [_ a b c d e f g h i j k l m n o p q r s t]      (select/select-one tbl a b c d e f g h i j k l m n o p q r s t))
  (invoke [_ a b c d e f g h i j k l m n o p q r s t args] (select/select-one tbl a b c d e f g h i j k l m n o p q r s t args))
  (applyTo [_ args]                                        (select/select-one tbl args))

  u/DispatchValue
  (dispatch-value [_]
    tbl)

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `->LegacyModel) model-name tbl m))

  hformat/ToSql
  (to-sql [_]
    (hformat/to-sql (compile/table-identifier tbl)))

  clojure.lang.Named
  (getName [_]
    model-name)
  (getNamespace [_]
    nil))

(alter-meta! #'->LegacyModel assoc :deprecated true)

(println "Adding Toucan support for" `LegacyModel)

(defn create-from-map [^LegacyModel legacy-model m]
  (if (instance? LegacyModel m)
    m
    (LegacyModel. (.model-name legacy-model) (.tbl legacy-model) m)))

(extend LegacyModel
  t.models/IModel
  t.models/IModelDefaults

  t.models/ICreateFromMap
  {:map-> create-from-map})

(println "Adding Blue JDBC support for Toucan models")
(m/defmethod tableable/table-name* [:default clojure.lang.IRecord]
  [connectable model options]
  (if (t.models/model? model)
    (name (:table model))
    (next-method connectable model options)))

(defn ^:deprecated legacy-model [model-name k table-name]
  (->LegacyModel model-name k (toucan-map model-name table-name)))

(defmacro ^:deprecated define-legacy-model
  "Define a Toucan-style 'model' that works with both Toucan and Blue JDBC."
  [symb k table-name]
  `(do
     (def ~(vary-meta symb assoc :deprecated true)
       (legacy-model ~(name symb) ~k ~table-name))
     (helpers/define-table-name ~k ~table-name)))
