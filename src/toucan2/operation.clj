(ns toucan2.operation
  "Common/shared code for implementing operations like the code in [[toucan2.insert]] or [[toucan2.update]]."
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

;;;; things that return update count

;;; TODO -- give this a name that makes it's clear for 'write' operations, not read ones.
(m/defmulti reducible*
  "Return a reducible query based on `parsed-args`. This should return either an update count (number of rows updated) or
  a sequence of PK values for affected rows; which of these gets returned depends on [[toucan2.jdbc.query/options]] (for
  the JDBC backend) or similar."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible* :default
  [query-type model parsed-args]
  (query/with-query [query [model (:queryable parsed-args)]]
    (let [built-query (query/build query-type model (-> parsed-args
                                                        (assoc :query query)
                                                        (dissoc :queryable)))]
      (execute/reducible-query (model/deferred-current-connectable model) model built-query))))

(defn reducible [query-type modelable unparsed-args]
  (model/with-model [model modelable]
    (let [parsed-args (query/parse-args query-type model unparsed-args)]
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
                    (format "Expected row to be a map, got ^%s %s" (some-> row class .getCanonicalName) (u/safe-pr-str row)))
            (u/with-debug-result ["%s: map pk function %s to row %s" `return-pks-eduction pks-fn row]
              (let [pks (pks-fn row)]
                (when (nil? pks)
                  (throw (ex-info (format "Error returning PKs: pks-fn returned nil for row %s" (u/safe-pr-str row))
                                  {:context u/*error-context*, :row (realize/realize row), :pks-fn pks-fn})))
                pks))))
     (execute/->WithReturnKeys reducible-operation))))

(m/defmulti reducible-returning-pks*
  "Given `parsed-args` return a reducible that when reduced yields a sequence of PK values for affected rows. Each PK
  value should be a plain value for rows with a single primary key, e.g. an integer for a row with an integer `:id`
  column, or a vector of primary key values in the same order as [[toucan2.model/primary-keys]] for models with
  composite primary keys.

  The default implementation combines [[reducible*]] with [[return-pks-eduction]]."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-returning-pks* :default
  [query-type model parsed-args]
  (return-pks-eduction model (reducible* query-type model parsed-args)))

(defn reducible-returning-pks
  "Helper wrapper for [[reducible-returning-pks*]] that also resolves `modelable` and parses `unparsed-args`."
  [query-type modelable unparsed-args]
  (model/with-model [model modelable]
    (let [parsed-args (query/parse-args query-type model unparsed-args)]
      (reducible-returning-pks* query-type model parsed-args))))

(defn returning-pks!
  "Helper wrapper for [[reducible-returning-pks]] that also fully realizes the results."
  [query-type modelable unparsed-args]
  (u/with-debug-result ["%s %s returning PKs" query-type modelable]
    (realize/realize (reducible-returning-pks query-type modelable unparsed-args))))

;;;; things that return instances

(m/defmulti reducible-returning-instances*
  "Given `parsed-args`, return a reducible that yields rows of `model`, as [[toucan2.instance]] maps.

  The default implementation builds a reducible that combines a [[reducible-returning-pks*]]
  and [[->ReducibleReturningInstancesFromPKs]]."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(defn select-reducible-with-pks
  "Return a reducible that returns instances of `model` that match a sequence of PKs. Each PK should be either a single
  value or vector of values (for models with a composite PK). This is used to implement the default implementation
  of [[reducible-returning-instances*]]."
  [model columns row-pks]
  (if (empty? row-pks)
    []
    (let [pk-vecs     (for [pk row-pks]
                        (if (sequential? pk)
                          pk
                          [pk]))
          pk-keys     (model/primary-keys model)
          pk-maps     (for [pk-vec pk-vecs]
                        (zipmap pk-keys pk-vec))
          kv-args  (into
                    {}
                    (map (fn [k]
                           [k [:in (mapv k pk-maps)]]))
                    pk-keys)
          parsed-args {:columns   columns
                       :kv-args   kv-args
                       :queryable {}}]
      (reducible-returning-instances* :toucan2.select/select model parsed-args))))

(deftype ^:no-doc ReducibleReturningInstancesFromPKs [model columns reducible-returning-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (when-let [row-pks (not-empty (realize/realize reducible-returning-pks))]
      (u/with-debug-result ["return instances for PKs %s" row-pks]
        (reduce
         rf
         init
         (select-reducible-with-pks model columns row-pks)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleReturningInstancesFromPKs model columns reducible-returning-pks)))

;;; the default implementation is for things like update or insert and is built in terms of taking a reducible returning
;;; PKs and then selecting them with a subsequent query. This doesn't make sense for things like `select` that can
;;; return instances directly -- it needs its own implementation.
(m/defmethod reducible-returning-instances* :default
  [query-type model {:keys [columns], :as parsed-args}]
  (let [reducible-returning-pks (reducible-returning-pks* query-type model (dissoc parsed-args :columns))]
    (->ReducibleReturningInstancesFromPKs model columns reducible-returning-pks)))

(defn reducible-returning-instances
  "Helper wrapper for [[reducible-returning-instances*]] that also resolves `model` and parses `unparsed-args`."
  [query-type modelable-columns unparsed-args]
  (let [[modelable & columns] (if (sequential? modelable-columns)
                                modelable-columns
                                [modelable-columns])]
    (model/with-model [model modelable]
      (let [parsed-args (assoc (query/parse-args query-type model unparsed-args)
                               :columns columns)]
        (reducible-returning-instances* query-type model parsed-args)))))

(defn returning-instances!
  "Helper wrapper for [[reducible-returning-instances]] that also fully realizes the reducible it returns."
  [query-type modelable-columns unparsed-args]
  (u/with-debug-result ["%s %s returning instances" query-type (if (sequential? modelable-columns)
                                                                 (first modelable-columns)
                                                                 modelable-columns)]
    (realize/realize (reducible-returning-instances query-type modelable-columns unparsed-args))))
