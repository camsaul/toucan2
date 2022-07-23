(ns toucan2.select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.util :as u]
   [toucan2.realize :as realize]))

(m/defmulti parse-args
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

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

(m/defmethod parse-args :default
  [_model args]
  (let [parsed (s/conform ::default-select-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain-str ::default-select-args args))
                      (s/explain-data ::default-select-args args))))
    (let [{:keys [query conditions]} parsed]
      [(into {} (map (juxt :k :v)) conditions)
       (if (nil? query)
         {}
         query)])))

(m/defmulti select-reducible*
  ;; the actual args depend on what [[parse-args]] returns
  {:arglists '([model columns & args])}
  u/dispatch-on-first-arg)

(m/defmethod select-reducible* :default
  [model columns conditions query]
  (let [query       (model/build-select-query model query columns conditions)
        connectable (model/default-connectable model)]
    (model/reducible-query-as connectable model query)))

(defn select-reducible [modelable & args]
  {:arglists '([modelable & conditions? query?]
               [[modelable & columns] & conditions? query?])}
  (let [[modelable & columns] (if (sequential? modelable)
                                modelable
                                [modelable])]
    (model/with-model [model modelable]
      (let [args (parse-args model args)]
        ;; TODO -- should we TRACE when debugging is enabled?
        (apply #_methodical.util.trace/trace* select-reducible* model columns args)))))

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

(defn select-pks-fn [modelable]
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

#_(m/defmulti count*
  {:arglists '([connectableᵈ modelableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

#_(m/defmethod count* :default
  [modelable query options]
  (log/tracef "No efficient implementation of count* for %s, doing select-reducible and counting the rows..."
              (u/dispatch-value query))
  (reduce
   (fn [acc _]
     (inc acc))
   0
   (select* modelable query options)))

#_(defn count
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (let [

        [connectable options]   (conn.current/ensure-connectable modelable nil)
        {:keys [query options]} (parse-args modelable args options)]
    (count* modelable query options)))

#_(m/defmulti exists?*
  {:arglists '([connectableᵈ modelableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

#_(m/defmethod exists?* :default
  [modelable query options]
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
   (select* modelable query options)))

#_(defn exists?
  {:arglists '([modelable & conditions? query?])}
  [modelable & args]
  (let [

        [connectable options]   (conn.current/ensure-connectable modelable nil)
        {:keys [query options]} (parse-args modelable args options)]
    (exists?* modelable query options)))
