(ns toucan2.select
  (:refer-clojure :exclude [count])
  (:require
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(m/defmethod query/build [::select :default clojure.lang.IPersistentMap]
  [query-type model {:keys [columns], :as parsed-args}]
  (let [parsed-args (-> parsed-args
                        (update :query (fn [query]
                                         (merge {:select (or (not-empty columns)
                                                             [:*])}
                                                (when model
                                                  {:from [[(keyword (model/table-name model))]]})
                                                query)))
                        (dissoc :columns))]
    (next-method query-type model parsed-args)))

(m/defmethod op/reducible-returning-instances* [::select :default]
  [query-type model parsed-args]
  (query/with-query [query [model (:queryable parsed-args)]]
    (let [query (query/build query-type model (assoc parsed-args :query query))]
      (execute/reducible-query (model/deferred-current-connectable model)
                               model
                               query))))

(defn reducible-select
  {:arglists '([modelable & kv-args? query?]
               [[modelable & columns] & kv-args? query?])}
  [modelable-columns & unparsed-args]
  (op/reducible-returning-instances ::select modelable-columns unparsed-args))

(defn select
  {:arglists '([modelable & kv-args? query?]
               [[modelable & columns] & kv-args? query?])}
  [modelable-columns & unparsed-args]
  (op/returning-instances! ::select modelable-columns unparsed-args))

(defn select-one {:arglists '([modelable & kv-args? query?]
                              [[modelable & columns] & kv-args? query?])}
  [modelable-columns & unparsed-args]
  (realize/reduce-first (apply reducible-select modelable-columns unparsed-args)))

(defn select-fn-reducible
  {:arglists '([f modelable & kv-args? query?])}
  [f & args]
  (eduction
   (map f)
   (apply reducible-select args)))

(defn select-fn-set
  "Like `select`, but returns a set of values of `(f instance)` for the results. Returns `nil` if the set is empty."
  {:arglists '([f modelable & kv-args? query?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-fn-reducible args))))

(defn select-fn-vec
  "Like `select`, but returns a vector of values of `(f instance)` for the results. Returns `nil` if the vector is
  empty."
  {:arglists '([f modelable & kv-args? query?])}
  [& args]
  (not-empty (reduce conj [] (apply select-fn-reducible args))))

(defn select-one-fn
  {:arglists '([f modelable & kv-args? query?])}
  [& args]
  (realize/reduce-first (apply select-fn-reducible args)))

(defn select-pks-reducible
  {:arglists '([modelable & kv-args? query?])}
  [modelable & args]
  (let [f (model/select-pks-fn modelable)]
    (apply select-fn-reducible f modelable args)))

(defn select-pks-set
  {:arglists '([modelable & kv-args? query?])}
  [& args]
  (not-empty (reduce conj #{} (apply select-pks-reducible args))))

(defn select-pks-vec
  {:arglists '([modelable & kv-args? query?])}
  [& args]
  (not-empty (reduce conj [] (apply select-pks-reducible args))))

(defn select-one-pk
  {:arglists '([modelable & kv-args? query?])}
  [& args]
  (realize/reduce-first (apply select-pks-reducible args)))

(defn select-fn->fn
  {:arglists '([f1 f2 modelable & kv-args? query?])}
  [f1 f2 & args]
  (not-empty
   (into
    {}
    (map (juxt f1 f2))
    (apply reducible-select args))))

(defn select-fn->pk
  {:arglists '([f modelable & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (model/select-pks-fn modelable)]
    (apply select-fn->fn f pks-fn modelable args)))

(defn select-pk->fn
  {:arglists '([f modelable & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (model/select-pks-fn modelable)]
    (apply select-fn->fn pks-fn f modelable args)))

;;; TODO -- [[count]] and [[exists?]] implementations seem kinda dumb, maybe we should just hand off to
;;; [[reducible-select]] by default so it can handle the parsing and stuff.

(m/defmulti count*
  {:arglists '([model unparsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod count* :default
  [model unparsed-args]
  (u/println-debug ["No efficient implementation of count* for %s, doing reducible-select and counting the rows..." model])
  (reduce
   (fn [acc _]
     (inc acc))
   0
   (apply reducible-select model unparsed-args)))

(defn count
  {:arglists '([modelable & kv-args? query?])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (count* model unparsed-args)))

(m/defmulti exists?*
  {:arglists '([model unparsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod exists?* :default
  [model unparsed-args]
  (u/println-debug ["No efficient implementation of exists?* for %s, doing reducible-select and seeing if it returns a row..." model])
  (transduce
   (take 1)
   (fn
     ([acc]
      acc)
     ([_ _]
      true))
   false
   (apply reducible-select model unparsed-args)))

(defn exists?
  {:arglists '([modelable & kv-args? query?])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (exists?* model unparsed-args)))
