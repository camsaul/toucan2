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

(m/defmulti reducible-update*
  "Return a reducible query based on `parsed-args`. This should return either an update count (number of rows updated) or
  a sequence of PK values for affected rows; which of these gets returned depends on [[toucan2.jdbc.query/options]] (for
  the JDBC backend) or similar.

  This is \"update\" in the sense that it is something that updates the database and returns an update count -- it could
  mean a SQL `UPDATE`, `INSERT`, or `DELETE`."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-update* :default
  [query-type model parsed-args]
  (query/with-resolved-query [query [model (:queryable parsed-args)]]
    (let [built-query (query/build query-type model (-> parsed-args
                                                        (assoc :query query)
                                                        (dissoc :queryable)))]
      (execute/reducible-query (model/deferred-current-connectable model) model built-query))))

(defn reducible-update [query-type unparsed-args]
  (let [{:keys [modelable], :as parsed-args} (query/parse-args query-type unparsed-args)]
    (model/with-model [model modelable]
      (reducible-update* query-type model (dissoc parsed-args :modelable)))))

(defn returning-update-count! [query-type unparsed-args]
  (u/try-with-error-context ["execute update returning count" {::query-type query-type, ::unparsed-args unparsed-args}]
    (u/with-debug-result ["%s returning update count" query-type]
      (reduce (fnil + 0 0) 0 (reducible-update query-type unparsed-args)))))

;;;; things that return PKs

(defn return-pks-eduction
  "Given a `reducible-operation` returning whatever (presumably returning affected row counts) wrap it in an eduction and
  in [[toucan2.execute/->WithReturnKeys]] so it returns a sequence of primary key vectors."
  [model reducible-operation]
  (let [pks-fn (model/select-pks-fn model)]
    (eduction
     (map (fn [row]
            (u/try-with-error-context [(format "%s: get PKs from row" `return-pks-eduction)
                                       {::model model, ::row row}]
              (assert (map? row)
                      (format "Expected row to be a map, got ^%s %s" (some-> row class .getCanonicalName) (u/safe-pr-str row)))
              (u/with-debug-result ["%s: map pk function %s to row %s" `return-pks-eduction pks-fn row]
                (let [pks (pks-fn row)]
                  (when (nil? pks)
                    (throw (ex-info (format "Error returning PKs: pks-fn returned nil for row %s" (u/safe-pr-str row))
                                    {:row (realize/realize row), :pks-fn pks-fn})))
                  pks)))))
     (execute/->WithReturnKeys reducible-operation))))

(m/defmulti reducible-update-returning-pks*
  "Given `parsed-args` return a reducible that when reduced yields a sequence of PK values for affected rows. Each PK
  value should be a plain value for rows with a single primary key, e.g. an integer for a row with an integer `:id`
  column, or a vector of primary key values in the same order as [[toucan2.model/primary-keys]] for models with
  composite primary keys.

  The default implementation combines [[reducible-update*]] with [[return-pks-eduction]]."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-update-returning-pks* :default
  [query-type model parsed-args]
  (return-pks-eduction model (reducible-update* query-type model parsed-args)))

(defn reducible-update-returning-pks
  "Helper wrapper for [[reducible-update-returning-pks*]] that also resolves `modelable` and parses `unparsed-args`."
  [query-type unparsed-args]
  (let [{:keys [modelable], :as parsed-args} (query/parse-args query-type unparsed-args)]
    (model/with-model [model modelable]
      (reducible-update-returning-pks* query-type model (dissoc parsed-args :modelable)))))

(defn update-returning-pks!
  "Helper wrapper for [[reducible-update-returning-pks]] that also fully realizes the results."
  [query-type unparsed-args]
  (u/try-with-error-context ["execute update returning PKs" {::query-type query-type, ::unparsed-args unparsed-args}]
    (u/with-debug-result ["%s returning PKs" query-type]
      (realize/realize (reducible-update-returning-pks query-type unparsed-args)))))

;;;; things that return instances

(m/defmulti reducible-returning-instances*
  "Given `parsed-args`, return a reducible that yields rows of `model`, as [[toucan2.instance]] maps.

  The default implementation builds a reducible that combines a [[reducible-update-returning-pks*]]
  and [[->ReducibleReturningInstancesFromPKs]].

  Dispatches off of `[query-type model]`."
  {:arglists '([query-type model parsed-args])}
  u/dispatch-on-first-two-args)

;;; this special type of select query is used so other things like [[toucan2.tools.transform]] knows that this select
;;; query is a result of [[select-reducible-with-pks]], and the values do not need to be transformed.
(derive ::return-instances-from-pks :toucan2.select/select)

(defn select-reducible-with-pks
  "Return a reducible that returns instances of `model` that match a sequence of PKs. Each PK should be either a single
  value or vector of values (for models with a composite PK). This is used to implement the default implementation
  of [[reducible-returning-instances*]].

  `row-pks` should be exactly as they come back from the DB -- not transformed in any way."
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
      (reducible-returning-instances* ::return-instances-from-pks model parsed-args))))

(deftype ^:no-doc ReducibleReturningInstancesFromPKs [model columns reducible-update-returning-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (when-let [row-pks (not-empty (realize/realize reducible-update-returning-pks))]
      (u/try-with-error-context [(format "reduce %s" `ReducibleReturningInstancesFromPKs)
                                 {::model model, ::columns columns, ::row-pks row-pks}]
        (u/with-debug-result ["return instances for PKs %s" row-pks]
          (reduce
           rf
           init
           (select-reducible-with-pks model columns row-pks))))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleReturningInstancesFromPKs model columns reducible-update-returning-pks)))

;;; the default implementation is for things like `update!` or `insert!` and is built in terms of taking a reducible
;;; returning PKs and then selecting them with a subsequent query. This doesn't make sense for things like `select` that
;;; can return instances directly -- it needs its own implementation.
(m/defmethod reducible-returning-instances* :default
  [query-type model {:keys [columns], :as parsed-args}]
  (let [reducible-update-returning-pks (reducible-update-returning-pks* query-type model (dissoc parsed-args :columns))]
    (->ReducibleReturningInstancesFromPKs model columns reducible-update-returning-pks)))

(defn reducible-returning-instances
  "Helper wrapper for [[reducible-returning-instances*]] that also resolves `model` and parses `unparsed-args`."
  [query-type unparsed-args]
  (let [{:keys [modelable], :as parsed-args} (query/parse-args query-type unparsed-args)]
    (model/with-model [model modelable]
      (reducible-returning-instances* query-type model (dissoc parsed-args :modelable)))))

;;; this is not named with a `!` because it is not necessarily destructive -- [[toucan2.select/select]] uses it
(defn returning-instances
  "Helper wrapper for [[reducible-returning-instances]] that also fully realizes the reducible it returns."
  [query-type unparsed-args]
  (u/try-with-error-context ["execute query returning instances" {::query-type query-type, ::unparsed-args unparsed-args}]
    (u/with-debug-result ["%s returning instances" query-type]
      (realize/realize (reducible-returning-instances query-type unparsed-args)))))
