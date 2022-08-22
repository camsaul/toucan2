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

(m/defmulti reducible-returning-pks*
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-returning-pks* :default
  [query-type model parsed-args]
  (select/return-pks-eduction model (reducible* query-type model parsed-args)))

(defn reducible-returning-pks
  [query-type modelable unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [query-type model unparsed-args]]
      (reducible-returning-pks* query-type model parsed-args))))

(defn returning-pks! [query-type modelable unparsed-args]
  (u/with-debug-result ["%s %s returning PKs" query-type modelable]
    (realize/realize (reducible-returning-pks query-type modelable unparsed-args))))

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
         (select/select-reducible-with-pks (into [model] columns) row-pks)))))

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
