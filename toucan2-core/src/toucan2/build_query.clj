(ns toucan2.build-query
  (:require [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.connectable.current :as conn.current]
            [toucan2.log :as log]
            [toucan2.queryable :as queryable]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

;; A "buildable query" is a query that we can programatically build using the methods in `toucan2.build-query`.
(derive :toucan2/buildable-query :toucan2/query)

(m/defmulti buildable-query*
  "`query-type` is one of `:select`, `:insert`, `:update`, or `:delete`."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ query-typeᵈ options])}
  u/dispatch-on-first-four-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod buildable-query* :default
  [connectable tableable query query-type options]
  (throw (ex-info (format "Don't know how to create a buildable %s query with %s"
                          query-type
                          (binding [*print-meta* true]
                            (pr-str query)))
                  {:connectable connectable
                   :tableable   tableable
                   :query       query
                   :query-type  query-type
                   :options     options})))

(defn can-create-buildable-query? [connectable tableable queryable query-type]
  (let [dispatch-value (m/dispatch-value buildable-query* connectable tableable queryable query-type)]
    (not= :default (:dispatch-value (meta (m/effective-primary-method buildable-query* dispatch-value))))))

(defn buildable-query? [query]
  (isa? (u/dispatch-value query) :toucan2/buildable-query))

(defn maybe-buildable-query
  [connectable tableable queryable query-type options]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        query                 (if queryable
                                (queryable/queryable connectable tableable queryable options)
                                {})
        can-create-buildable? (can-create-buildable-query? connectable tableable query query-type)
        query                 (if can-create-buildable?
                                (log/with-trace ["Create buildable query from %s" query]
                                  (buildable-query* connectable tableable query query-type options))
                                (do
                                  (log/tracef "Cannot create a buildable query from %s" query)
                                  query))]
    (when can-create-buildable?
      (assert (buildable-query? query) (format "Expected a buildable query, got: %s"
                                               (binding [*print-meta* true]
                                                 (pr-str query)))))
    query))

(m/defmulti table*
  {:arglists '([queryᵈᵗ])}
  u/dispatch-on-first-arg)

(m/defmethod table* :default
  [query]
  (log/tracef "%s is not a buildable query; returning nil for table*" query)
  nil)

(m/defmulti with-table*
  {:arglists '([queryᵈᵗ new-table options])}
  u/dispatch-on-first-arg
  :combo (m/thread-first-method-combination))

(m/defmethod with-table* :default
  [query new-table _]
  (log/tracef "%s is not a buildable query; ignoring with-table* %s" query new-table)
  query)

(m/defmulti conditions*
  "Get the conditions (e.g. HoneySQL `:where` associated with a `query`. The actual representation of the conditions my
  vary depending on the query compilation backend."
  {:arglists '([queryᵈᵗ])}
  u/dispatch-on-first-arg)

(m/defmulti with-conditions*
  "Return a copy of `query` with its conditions replaced with `new-conditions`. The actual representation of the
  conditions my vary depending on the query compilation backend."
  {:arglists '([queryᵈᵗ new-conditions options]), :style/indent :form}
  u/dispatch-on-first-arg
  :combo (m/thread-first-method-combination))

(m/defmethod with-conditions* :default
  [query new-conditions _]
  (log/tracef "%s is not a buildable query; ignoring with-conditions* %s" query new-conditions)
  query)

(m/defmulti merge-kv-conditions*
  "Merge `kv-conditions` into a query. `kv-conditions` are passed as a map of `column-name-keyword` -> `value`. The
  query compilation backend should merge the equivalent of `WHERE col = value` clauses into query, conjoined by
  `AND`."
  {:arglists '([queryᵈᵗ kv-conditions options])}
  u/dispatch-on-first-arg
  :combo (m/thread-first-method-combination))

(m/defmethod merge-kv-conditions* :default
  [query kv-conditions _]
  (log/tracef "%s is not a buildable query; ignoring merge-kv-conditions* %s" query kv-conditions)
  query)

(m/defmethod merge-kv-conditions* :before :default
  [query kv-conditions _]
  (assert (or (nil? kv-conditions)
              (and (map? kv-conditions)
                   (every? keyword? (keys kv-conditions))))
          (format "Invalid kv-conditions, expected either nil or map of keyword -> condition, got: %s"
                  (binding [*print-meta* true] (pr-str kv-conditions))))
  query)

(m/defmulti rows*
  "Get the rows (e.g. HoneySQL `:values`) associated with an `insert-query`."
  {:arglists '([insert-queryᵈᵗ])}
  u/dispatch-on-first-arg)

(m/defmulti with-rows*
  "Return a copy of `insert-query` with its rows replaced with `new-rows`."
  {:arglists '([insert-queryᵈᵗ new-rows options]), :style/indent :form}
  u/dispatch-on-first-arg
  :combo (m/thread-first-method-combination))

(m/defmethod with-rows* :default
  [query rows _]
  (log/tracef "%s is not a buildable query; ignoring with-rows* %s" query rows)
  query)

(m/defmulti changes*
  "Return the updates (e.g. HoneySQL `:set`) that will be done to rows matching [[toucan2.build-query/conditions*]] for
  an `update-query`."
  {:arglists '([update-queryᵈᵗ])}
  u/dispatch-on-first-arg)

(m/defmulti with-changes*
  "Return a copy of `update-query` with its changes that should be performed set to `new-changes`."
  {:arglists '([update-queryᵈᵗ new-changes options]), :style/indent :form}
  u/dispatch-on-first-arg
  :combo (m/thread-first-method-combination))

(m/defmethod with-changes* :default
  [query new-changes _]
  (log/tracef "%s is not a buildable query; ignoring with-changes* %s" query new-changes)
  query)

(defn merge-primary-key
  "Merge primary key `pk-vals` either a single value like `1` for a single-column PK or a sequence of values for a PK
  consisting of one or more columns) into a `kv-conditions` map."
  [connectable tableable kv-conditions pk-vals options]
  (log/with-trace ["Adding primary key values %s" (pr-str pk-vals)]
    (let [pk-cols (tableable/primary-key-keys connectable tableable)
          _       (log/tracef "Primary key(s) for %s is %s" (pr-str tableable) (pr-str pk-cols))
          pk-vals (if (sequential? pk-vals)
                    pk-vals
                    [pk-vals])
          pk-map  (zipmap pk-cols pk-vals)]
      (merge kv-conditions pk-map))))
