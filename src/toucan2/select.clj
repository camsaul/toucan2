(ns toucan2.select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.util :as u]
   [toucan2.realize :as realize]))

(s/def ::default-select-args
  (s/cat ;; :modelable  (s/or
         ;;              :model          (complement sequential?)
         ;;              :model-and-cols (s/cat
         ;;                               :model any?
         ;;                               :cols  (s/* any?)))
         :conditions (s/* (s/cat
                           :k keyword?
                           :v (complement map?)))
         :query      (s/? any?)))

(defn parse-select-args [args]
  (let [parsed (s/conform ::default-select-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain-str ::default-select-args args))
                      (s/explain-data ::default-select-args args))))
    (cond-> parsed
      (nil? (:query parsed))     (assoc :query {})
      (seq (:conditions parsed)) (update :conditions (fn [conditions]
                                                       (into {} (map (juxt :k :v)) conditions))))))

(m/defmulti select-reducible*
  {:arglists '([model columns args])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod select-reducible* :default
  [model columns args]
  (let [{:keys [conditions query]} (parse-select-args args)
        query                      (model/build-select-query model query columns conditions)
        connectable                (model/default-connectable model)]
    (model/reducible-query-as connectable model query)))

(defn select-reducible [modelable & args]
  {:arglists '([modelable & conditions? query?]
               [[modelable & columns] & conditions? query?])}
  (let [[modelable & columns] (if (sequential? modelable)
                                modelable
                                [modelable])]
    (model/with-model [model modelable]
      (select-reducible* model columns args))))

(defn select
  {:arglists '([modelable & conditions? query?]
               [[modelable & columns] & conditions? query?])}
  [modelable & args]
  (realize/realize (apply select-reducible modelable args)))

(defn select-one {:arglists '([modelable & conditions? query?]
                              [[modelable & columns] & conditions? query?])}
  [modelable & args]
  (first
   (realize/realize
    (eduction
     (take 1)
     (apply select-reducible modelable args)))))

#_(defn select-fn-reducible
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f & args]
  (eduction
   (map f)
   (apply select-reducible args)))

#_(defn select-fn-set
  "Like `select`, but returns a set of values of `(f instance)` for the results. Returns `nil` if the set is empty."
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-fn-reducible args))))

#_(defn select-fn-vec
  "Like `select`, but returns a vector of values of `(f instance)` for the results. Returns `nil` if the vector is
  empty."
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj [] (apply select-fn-reducible args))))

#_(defn select-one-fn
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-fn-reducible args)))

#_(defn select-pks-fn [connectable tableable]
  (let [pk-keys (tableable/primary-key-keys connectable tableable)]
    (if (= (clojure.core/count pk-keys) 1)
      (first pk-keys)
      (apply juxt pk-keys))))

#_(defn select-pks-reducible
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        f                       (select-pks-fn connectable tableable)]
    (apply select-fn-reducible f [connectable tableable] args)))

#_(defn select-pks-set
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-pks-reducible args))))

#_(defn select-pks-vec
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (not-empty (reduce conj [] (apply select-pks-reducible args))))

#_(defn select-one-pk
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (query/reduce-first (apply select-pks-reducible args)))

#_(defn select-fn->fn
  {:arglists '([f1 f2 connectable-tableable pk? & conditions? queryable? options?])}
  [f1 f2 & args]
  (not-empty
   (into
    {}
    (map (juxt f1 f2))
    (apply select-reducible args))))

#_(defn select-fn->pk
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        pks-fn                  (select-pks-fn connectable tableable)]
    (apply select-fn->fn f pks-fn [connectable tableable] args)))

#_(defn select-pk->fn
  {:arglists '([f connectable-tableable pk? & conditions? queryable? options?])}
  [f connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        pks-fn                  (select-pks-fn connectable tableable)]
    (apply select-fn->fn pks-fn f [connectable tableable] args)))

#_(m/defmulti count*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

#_(m/defmethod count* :default
  [connectable tableable query options]
  (log/tracef "No efficient implementation of count* for %s, doing select-reducible and counting the rows..."
              (u/dispatch-value query))
  (reduce
   (fn [acc _]
     (inc acc))
   0
   (select* connectable tableable query options)))

#_(defn count
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [query options]} (parse-select-args connectable tableable args options)]
    (count* connectable tableable query options)))

#_(m/defmulti exists?*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

#_(m/defmethod exists?* :default
  [connectable tableable query options]
  (log/tracef "No efficient implementation of exists?* for %s, doing select-reducible and seeing if it returns a row..."
              (u/dispatch-value query))
  (transduce
   (take 1)
   (fn
     ([acc]
      acc)
     ([_ _]
      true))
   false
   (select* connectable tableable query options)))

#_(defn exists?
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [query options]} (parse-select-args connectable tableable args options)]
    (exists?* connectable tableable query options)))
