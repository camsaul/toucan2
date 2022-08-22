(ns toucan2.operation
  "Common/shared code for implementing operations like the code in [[toucan2.insert]] or [[toucan2.update]]."
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.util :as u]))

;;;; things that return update count

;;; TODO -- should this be a multimethod?
(defn- reduce-reducible [query-type model parsed-args rf init]
  (let [query           (query/build query-type model parsed-args)
        reducible-query (execute/reducible-query (model/deferred-current-connectable model) model query)]
    (u/with-debug-result ["Reducing %s for model %s" query-type model]
      (reduce rf init reducible-query))))

(defrecord ReducibleOperation [query-type model parsed-args]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce-reducible query-type model parsed-args rf init))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleOperation query-type model parsed-args)))

(m/defmulti reducible*
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible* :default
  [query-type model parsed-args]
  (->ReducibleOperation query-type model parsed-args))

(defn reducible [query-type modelable unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [query-type model unparsed-args]]
      (reducible* query-type model parsed-args))))

(defn returning-update-count! [query-type modelable unparsed-args]
  (u/with-debug-result ["%s %s returning update count" query-type modelable]
    (reduce (fnil + 0 0) 0 (reducible query-type modelable unparsed-args))))

;;;; things that return PKs

(defn return-pks-eduction
  "Given a `reducible-operation` returning whatever (presumably returning affected row counts) wrap it in an eduction and
  in [[->WithReturnKeys]] so it returns a sequence of primary key vectors."
  [model reducible-operation]
  (let [pks-fn (model/select-pks-fn model)]
    (eduction
     (map (fn [row]
            (assert (map? row)
                    (format "Expected row to be a map, got ^%s %s" (some-> row class .getCanonicalName) (pr-str row)))
            (u/with-debug-result ["%s: map pk function %s to row %s" `return-pks-eduction pks-fn row]
              (let [pks (pks-fn row)]
                (when (nil? pks)
                  (throw (ex-info (format "Error returning PKs: pks-fn returned nil for row %s" (pr-str row))
                                  {:row (realize/realize row), :pks-fn pks-fn})))
                pks))))
     (execute/->WithReturnKeys reducible-operation))))

(m/defmulti reducible-returning-pks*
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-returning-pks* :default
  [query-type model parsed-args]
  (return-pks-eduction model (reducible* query-type model parsed-args)))

(defn reducible-returning-pks
  [query-type modelable unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [query-type model unparsed-args]]
      (reducible-returning-pks* query-type model parsed-args))))

(defn returning-pks! [query-type modelable unparsed-args]
  (u/with-debug-result ["%s %s returning PKs" query-type modelable]
    (realize/realize (reducible-returning-pks query-type modelable unparsed-args))))

;;;; things that return instances

(defn select-reducible-with-pks
  "Return a [[select/select-reducible]] for instances of `model` that match a sequence of PKs. Each PK should be either a
  single value or vector of values (for models with a composite PK)."
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
          (apply select/select-reducible model-columns conditions))))))

(m/defmulti reducible-returning-instances*
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(defrecord ReducibleReturningInstances [model columns reducible-returning-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (when-let [row-pks (not-empty (realize/realize reducible-returning-pks))]
      (u/with-debug-result ["return instances for PKs %s" row-pks]
        (reduce
         rf
         init
         (select-reducible-with-pks (into [model] columns) row-pks)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleReturningInstances model columns reducible-returning-pks)))

(m/defmethod reducible-returning-instances* :default
  [query-type model {:keys [columns], :as parsed-args}]
  (let [reducible-returning-pks (reducible-returning-pks* query-type model parsed-args)]
    (->ReducibleReturningInstances model columns reducible-returning-pks)))

(defn reducible-returning-instances [query-type modelable-columns unparsed-args]
  (let [[modelable & columns] (if (sequential? modelable-columns)
                                modelable-columns
                                [modelable-columns])]
    (model/with-model [model modelable]
      (query/with-parsed-args-with-query [parsed-args [query-type model unparsed-args]]
        (reducible-returning-instances* query-type model (assoc parsed-args :columns columns))))))

(defn returning-instances! [query-type modelable-columns unparsed-args]
  (u/with-debug-result ["%s %s returning instances" query-type (if (sequential? modelable-columns)
                                                                 (first modelable-columns)
                                                                 modelable-columns)]
    (realize/realize (reducible-returning-instances query-type modelable-columns unparsed-args))))
