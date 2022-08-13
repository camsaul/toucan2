(ns toucan2.select
  (:refer-clojure :exclude [count])
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(m/defmulti parse-args
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(s/def ::default-args
  (s/cat
   :conditions (s/* (s/cat
                     :k any?
                     :v any?))
   :query      (s/? any?)))

(m/defmethod parse-args :default
  [_model args]
  (let [parsed (s/conform ::default-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain-str ::default-args args))
                      (s/explain-data ::default-args args))))
    (cond-> parsed
      (nil? (:query parsed))     (assoc :query {})
      (seq (:conditions parsed)) (update :conditions (fn [conditions]
                                                       (into {} (map (juxt :k :v)) conditions))))))

;;; TODO -- not 100% sure this needs to be a separate multimethod -- the equivalents for `update!`, `insert!`, and
;;; `delete!` are not.
;;;
;;; TODO -- this should probably avoid destructuring the `args` map in case another implementation has different args.
(m/defmulti build-query
  {:arglists '([model query columns conditions])}
  u/dispatch-on-first-two-args)

(m/defmethod build-query :around :default
  [model query columns conditions]
  (u/with-debug-result (pr-str (list `build-query model query columns conditions))
    (next-method model query columns conditions)))

(m/defmethod build-query :default
  [model query columns conditions]
  (when (or (seq columns)
            (seq conditions))
    (throw (ex-info (format (str "Don't know how to build select query for %s from query ^%s %s with columns or "
                                 "conditions. Do you need to implement %s for %s?")
                            (pr-str model)
                            (some-> query class .getCanonicalName)
                            (pr-str query)
                            `build-query
                            (pr-str (u/dispatch-on-first-two-args model query)))
                    {:model model, :query query, :columns columns, :conditions conditions})))
  query)

(m/defmethod build-query [:default clojure.lang.IPersistentMap]
  [model query columns conditions]
  (let [honeysql (merge {:select (or (not-empty columns)
                                     [:*])}
                        (when model
                          {:from [[(keyword (model/table-name model))]]})
                        query)]
    (compile/apply-conditions model honeysql conditions)))

(m/defmethod build-query [:default Long]
  [model pk columns conditions]
  (build-query model {} columns (assoc conditions :toucan/pk pk)))

(m/defmulti select-reducible*
  ;; the actual args depend on what [[parse-args]] returns
  {:arglists '([model columns args])}
  u/dispatch-on-first-arg)

(m/defmethod select-reducible* :default
  [model columns {:keys [conditions query], :as _args}]
  (let [query (build-query model query columns conditions)]
    (query/reducible-query-as model query)))

(defn select-reducible [modelable & args]
  {:arglists '([modelable & conditions? query?]
               [[modelable & columns] & conditions? query?])}
  (let [[modelable & columns] (if (sequential? modelable)
                                modelable
                                [modelable])]
    (model/with-model [model modelable]
      ;; TODO -- should we [[m/trace]] when debugging is enabled?
      (select-reducible* model columns (parse-args model args)))))

(defn select
  {:arglists '([modelable & conditions? query?]
               [[modelable & columns] & conditions? query?])}
  [modelable & args]
  (realize/realize (apply select-reducible modelable args)))

(defn select-one {:arglists '([modelable & conditions? query?]
                              [[modelable & columns] & conditions? query?])}
  [modelable & args]
  (realize/reduce-first (apply select-reducible modelable args)))

(defn select-fn-reducible
  {:arglists '([f modelable & conditions? query?])}
  [f & args]
  (eduction
   (map f)
   (apply select-reducible args)))

(defn select-fn-set
  "Like `select`, but returns a set of values of `(f instance)` for the results. Returns `nil` if the set is empty."
  {:arglists '([f modelable & conditions? query?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-fn-reducible args))))

(defn select-fn-vec
  "Like `select`, but returns a vector of values of `(f instance)` for the results. Returns `nil` if the vector is
  empty."
  {:arglists '([f modelable & conditions? query?])}
  [& args]
  (not-empty (reduce conj [] (apply select-fn-reducible args))))

(defn select-one-fn
  {:arglists '([f modelable & conditions? query?])}
  [& args]
  (realize/reduce-first (apply select-fn-reducible args)))

(defn select-pks-fn
  "Return a function to get the value(s) of the primary key(s) from a row. Used by [[select-pks-reducible]] and thus
  by [[select-pks-set]], [[select-pks-vec]], etc.

  The primary keys are determined by [[model/primary-keys]]. By default this is simply the keyword `:id`."
  [modelable]
  (let [pk-keys (model/primary-keys modelable)]
    (if (= (clojure.core/count pk-keys) 1)
      (first pk-keys)
      (apply juxt pk-keys))))

(defn select-pks-reducible
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (let [f (select-pks-fn modelable)]
    (apply select-fn-reducible f modelable args)))

(defn select-pks-set
  {:arglists '([modelable & conditions? query?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-pks-reducible args))))

(defn select-pks-vec
  {:arglists '([modelable & conditions? query?])}
  [& args]
  (not-empty (reduce conj [] (apply select-pks-reducible args))))

(defn select-one-pk
  {:arglists '([modelable & conditions? query?])}
  [& args]
  (realize/reduce-first (apply select-pks-reducible args)))

(defn select-fn->fn
  {:arglists '([f1 f2 modelable & conditions? query?])}
  [f1 f2 & args]
  (not-empty
   (into
    {}
    (map (juxt f1 f2))
    (apply select-reducible args))))

(defn select-fn->pk
  {:arglists '([f modelable & conditions? query?])}
  [f modelable & args]
  (let [pks-fn (select-pks-fn modelable)]
    (apply select-fn->fn f pks-fn modelable args)))

(defn select-pk->fn
  {:arglists '([f modelable & conditions? query?])}
  [f modelable & args]
  (let [pks-fn (select-pks-fn modelable)]
    (apply select-fn->fn pks-fn f modelable args)))

(m/defmulti count*
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod count* :default
  [model args]
  (u/println-debug (format "No efficient implementation of count* for %s, doing select-reducible and counting the rows..."
                           (pr-str model)))
  (reduce
   (fn [acc _]
     (inc acc))
   0
   (select-reducible* model nil args)))

(defn count
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (model/with-model [model modelable]
    (count* model (parse-args model args))))

(m/defmulti exists?*
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod exists?* :default
  [model args]
  (u/println-debug (format "No efficient implementation of exists?* for %s, doing select-reducible and seeing if it returns a row..."
                           (pr-str model)))
  (transduce
   (take 1)
   (fn
     ([acc]
      acc)
     ([_ _]
      true))
   false
   (select-reducible* model nil args)))

(defn exists?
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (model/with-model [model modelable]
    (exists?* model (parse-args model args))))
