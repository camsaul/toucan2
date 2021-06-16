(ns toucan.models
  "The `defmodel` macro, used to define Toucan models, and
   the `IModel` protocol and default implementations, which implement Toucan model functionality."
  (:require [honeysql.format :as hformat]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]
            [toucan.common :as common]
            [toucan2.helpers :as helpers]
            [toucan2.honeysql.compile :as honeysql.compile]
            [toucan2.identity-query :as identity-query]
            [toucan2.instance :as instance]
            [toucan2.log :as log]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u])
  (:import honeysql.format.ToSql))

(defn wrapped-transform [k f]
  (fn [v]
    (log/with-trace ["Transform %s %s" k (pr-str v)]
      (try
        (f v)
        (catch Throwable e
          (throw (ex-info (format "Error transforming %s %s: %s" k (pr-str v) (ex-message e))
                          {:k k, :v v}
                          e)))))))

(defmacro add-type! [k & {:keys [in out], :as m}]
  `(def ~(vary-meta (symbol (format "%s-xform" (name k))) assoc :deprecated true)
     ~(format "The type transform formerly known as %s." k)
     ~(cond-> m
        in (update :in (fn [f]
                         `(wrapped-transform ~k ~f)))
        out (update :out (fn [f]
                           `(wrapped-transform ~k ~f))))))

(defmacro add-property!
  {:style/indent 1}
  [k & {:keys [insert update select]}]
  (let [property-keyword (keyword "toucan1.models.properties" (name k))]
    `(do
       ~(when insert
          `(let [insert-fn# ~insert]
             (helpers/define-before-insert ~property-keyword
               [instance#]
               (insert-fn# instance# nil))))
       ~(when update
          `(let [update-fn# ~update]
             (helpers/define-before-update ~property-keyword
               [instance#]
               (update-fn# instance# nil))))
       ~(when select
          `(let [select-fn# ~select]
             (helpers/define-before-select ~property-keyword
               [instance#]
               (select-fn# instance# nil)))))))

(defn primary-key [model]
  (let [model (common/resolve-model model)]
    (first (tableable/primary-key-keys nil model))))

#_(defn do-pre-insert
  "Don't call this directly! Apply functions like `pre-insert` before inserting an object into the DB."
  [model obj]
  (as-> obj <>
    (map-> model <>)
    (pre-insert <>)
    (map-> model <>)
    (apply-type-fns <> :in)
    (apply-property-fns :insert <>)))

#_(defn do-pre-update
  "Don't call this directly! Apply internal functions like `pre-update` before updating an object in the DB."
  [model obj]
  (as-> obj <>
    (map-> model <>)
    (pre-update <>)
    (map-> model <>)
    (apply-type-fns <> :in)
    (apply-property-fns :update <>)))

(defn do-post-select
  "Don't call this directly! Apply internal functions like `post-select` when an object is retrieved from the DB."
  [model obj]
  (select/select-one (identity-query/identity-query [obj])))

(defn- toucan-map [model-name table-name]
  {:table  (keyword table-name)
   :name   model-name
   ::model true})

(defn model? [x]
  (::model x))

(p/deftype+ ^:deprecated LegacyModel [model-name tbl ^clojure.lang.IPersistentMap m]
  clojure.lang.ILookup
  (valAt [_ k]
    (.valAt m k))
  (valAt [_ k default]
    (.valAt m k default))

  clojure.lang.IObj
  (meta [_]
    (meta m))
  (withMeta [_ new-meta]
    (LegacyModel. model-name tbl (with-meta m new-meta)))

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
  (applyTo [_ args]                                        (apply select/select-one tbl args))

  u/DispatchValue
  (dispatch-value [_]
    tbl)

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `->LegacyModel) model-name tbl m))

  hformat/ToSql
  (to-sql [_]
    (hformat/to-sql (honeysql.compile/table-identifier tbl)))

  clojure.lang.Named
  (getName [_]
    model-name)
  (getNamespace [_]
    nil))

(m/defmethod tableable/table-name* [:default clojure.lang.IRecord]
  [connectable model options]
  (if (model? model)
    (name (:table model))
    (next-method connectable model options)))

(defn ^:deprecated legacy-model [model-name k table-name]
  (->LegacyModel model-name k (with-meta (toucan-map model-name table-name) {:type k})))

(defmacro ^:deprecated define-legacy-model
  "Define a Toucan-style 'model' that works with both Toucan and Blue JDBC."
  [symb k table-name]
  `(do
     (def ~(vary-meta symb assoc :deprecated true)
       (legacy-model ~(name symb) ~k ~table-name))
     (derive ~k :toucan1/legacy-model)
     (helpers/define-table-name ~k ~table-name)))

(defmacro defmodel
  ([model table-name]
   `(defmodel ~model nil ~table-name))
  ([model docstring table-name]
   (let [model-key (keyword "models" (name model))]
     `(do
        (define-legacy-model ~model ~model-key ~(name table-name))
        ~(when docstring
           `(alter-meta! (var ~model) assoc :doc ~docstring))))))

;; instance should use actual model e.g. :models/User for its tableable rather than the LegacyModel map
(m/defmethod instance/instance* [:default :toucan1/legacy-model]
  [connectable tableable original-map current-map key-xform metta]
  (if (instance? LegacyModel tableable)
    (instance/instance* connectable (u/dispatch-value tableable) original-map current-map key-xform metta)
    (next-method connectable tableable original-map current-map key-xform metta)))


(doseq [[_ varr] (ns-interns *ns*)]
  (alter-meta! varr assoc :deprecated true))
