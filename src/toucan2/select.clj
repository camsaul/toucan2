(ns toucan2.select
  (:refer-clojure :exclude [count])
  (:require
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(m/defmethod query/build [::select :default clojure.lang.IPersistentMap]
  [query-type model {:keys [columns], :as args}]
  (let [args (update args :query (fn [query]
                                   (merge {:select (or (not-empty columns)
                                                       [:*])}
                                          (when model
                                            {:from [[(keyword (model/table-name model))]]})
                                          query)))]
    (next-method query-type model args)))

(m/defmulti select-reducible*
  "The actual args depend on what [[query/parse-args]] returns."
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod select-reducible* :default
  [model parsed-args]
  (let [query (query/build ::select model parsed-args)]
    (execute/reducible-query (model/deferred-current-connectable model)
                             model
                             query)))

(defn select-reducible [modelable-columns & unparsed-args]
  {:arglists '([modelable & kv-args? query?]
               [[modelable & columns] & kv-args? query?])}
  (u/with-debug-result [(list* `select-reducible modelable-columns unparsed-args)]
    (let [[modelable & columns] (if (sequential? modelable-columns)
                                  modelable-columns
                                  [modelable-columns])]
      (model/with-model [model modelable]
        (query/with-parsed-args-with-query [parsed-args [::select model unparsed-args]]
          (select-reducible* model (assoc parsed-args :columns columns)))))))

(defn select
  {:arglists '([modelable & kv-args? query?]
               [[modelable & columns] & kv-args? query?])}
  [modelable & args]
  (u/with-debug-result [(list* `select args)]
    (realize/realize (apply select-reducible modelable args))))

(defn select-one {:arglists '([modelable & kv-args? query?]
                              [[modelable & columns] & kv-args? query?])}
  [modelable & args]
  (realize/reduce-first (apply select-reducible modelable args)))

(defn select-fn-reducible
  {:arglists '([f modelable & kv-args? query?])}
  [f & args]
  (eduction
   (map f)
   (apply select-reducible args)))

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
  {:arglists '([modelable & kv-args? query?])}
  [modelable & args]
  (let [f (select-pks-fn modelable)]
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
    (apply select-reducible args))))

(defn select-fn->pk
  {:arglists '([f modelable & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (select-pks-fn modelable)]
    (apply select-fn->fn f pks-fn modelable args)))

(defn select-pk->fn
  {:arglists '([f modelable & kv-args? query?])}
  [f modelable & args]
  (let [pks-fn (select-pks-fn modelable)]
    (apply select-fn->fn pks-fn f modelable args)))

;;; TODO -- [[count]] and [[exists?]] implementations seem kinda dumb, maybe we should just hand off to
;;; [[select-reducible]] by default so it can handle the parsing and stuff.

(m/defmulti count*
  {:arglists '([model unparsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod count* :default
  [model unparsed-args]
  (u/println-debug ["No efficient implementation of count* for %s, doing select-reducible and counting the rows..." model])
  (reduce
   (fn [acc _]
     (inc acc))
   0
   (apply select-reducible model unparsed-args)))

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
  (u/println-debug ["No efficient implementation of exists?* for %s, doing select-reducible and seeing if it returns a row..." model])
  (transduce
   (take 1)
   (fn
     ([acc]
      acc)
     ([_ _]
      true))
   false
   (apply select-reducible model unparsed-args)))

(defn exists?
  {:arglists '([modelable & kv-args? query?])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (exists?* model unparsed-args)))

(defn select-reducible-with-pks
  "Return a [[select-reducible]] for instances of `model` that match a sequence of PKs. Each PK should be either a single
  value or vector of values (for models with a composite PK)."
  {:arglists '([model row-pks]
               [[model & columns] row-pks])}
  [modelable-columns row-pks]
  (if (empty? row-pks)
    []
    (let [[modelable & columns] (if (sequential? modelable-columns)
                                  modelable-columns
                                  [modelable-columns])]
      (model/with-model [model modelable]
        (let [pk-vecs       (for [pk row-pks]
                              (if (sequential? pk)
                                pk
                                [pk]))
              pk-keys       (model/primary-keys-vec model)
              pk-maps       (for [pk-vec pk-vecs]
                              (zipmap pk-keys pk-vec))
              conditions    (mapcat
                             (juxt identity (fn [k]
                                              [:in (mapv k pk-maps)]))
                             pk-keys)
              model-columns (if (seq columns)
                              (cons model columns)
                              model)]
          (apply select-reducible model-columns conditions))))))

(defn return-pks-eduction
  "Given a `reducible-operation` returning whatever (presumably returning affected row counts) wrap it in an eduction and
  in [[->WithReturnKeys]] so it returns a sequence of primary key vectors."
  [model reducible-operation]
  (let [pks-fn (select-pks-fn model)]
    (eduction
     (map (fn [row]
            (u/with-debug-result ["%s: map pk function %s to row %s" `return-pks-eduction pks-fn row]
              (let [pks (pks-fn row)]
                (when (nil? pks)
                  (throw (ex-info (format "Error returning PKs: pks-fn returned nil for row %s" (pr-str row))
                                  {:row (realize/realize row), :pks-fn pks-fn})))
                pks))))
     (execute/->WithReturnKeys reducible-operation))))
