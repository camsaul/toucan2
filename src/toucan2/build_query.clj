(ns toucan2.build-query
  (:require [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.compile :as compile]
            [toucan2.connectable.current :as conn.current]
            [toucan2.log :as log]
            [toucan2.queryable :as queryable]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

;; A "buildable query" is a query that we can programatically build using the methods in `toucan2.build-query`.
(derive :toucan2/buildable-query :toucan2/query)

(m/defmulti ^:deprecated merge-queries*
  "Merge two buildable queries together into a single query.
  DEPRECATED -- do away with `merge-queries*` and just programatically build a query with `with-table*` and the like
  instead of trying to merge stuff together."
  {:arglists '([query-1ᵈ query-2ᵈᵗ])}
  u/dispatch-on-first-two-args)

(m/defmethod merge-queries* :default
  [query-1 query-2]
  (throw (ex-info (binding [*print-meta* true]
                    (format "Don't know how to merge queries\n%s\nand\n%s\n%s"
                            (pr-str query-1)
                            (pr-str query-2)
                            (u/suggest-dispatch-values query-1 query-2)))
                  {:query-1 query-1, :query-2 query-2})))

(m/defmethod merge-queries* [:toucan2/buildable-query nil]
  [query _]
  query)

(m/defmethod merge-queries* [nil :toucan2/buildable-query]
  [_ query]
  query)

(m/defmethod merge-queries* [:toucan2/buildable-query clojure.lang.IPersistentMap]
  [query m]
  (merge query m))

(m/defmethod merge-queries* [clojure.lang.IPersistentMap :toucan2/buildable-query]
  [m query]
  (with-meta (merge m query) (meta query)))

(m/defmethod merge-queries* [:toucan2/buildable-query :toucan2/buildable-query]
  [query-1 query-2]
  (merge query-1 query-2))

(m/defmulti table*
  "Get the table in a query."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ])}
  u/dispatch-on-first-three-args)

(m/defmulti with-table*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ new-table options]), :style/indent :form}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod with-table* :default
  [connectable tableable query new-table _]
  (throw (ex-info (format "Don't know how to add table to query %s.\n%s"
                          (binding [*print-meta* true] (pr-str query))
                          (u/suggest-dispatch-values connectable tableable query))
                  {:tableable tableable, :query query, :new-table new-table})))

;; if Query is a string (e.g. SQL), with-table should no-op
(m/defmethod with-table* [:default :default String]
  [_ _ query new-table _]
  (log/tracef "Query is a String; not adding table %s" (pr-str new-table))
  query)

(defn with-table [connectable tableable queryable new-table options]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        query                 (queryable/queryable connectable tableable queryable options)]
    (with-table* connectable tableable query new-table options)))

(m/defmulti conditions*
  "Get the conditions (e.g. HoneySQL `:where` associated with a `query`. The actual representation of the conditions my
  vary depending on the query compilation backend."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ])}
  u/dispatch-on-first-three-args)

(m/defmulti with-conditions*
  "Return a copy of `query` with its conditions replaced with `new-conditions`. The actual representation of the
  conditions my vary depending on the query compilation backend."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ new-conditions options]), :style/indent :form}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmulti merge-kv-conditions*
  "Merge `kv-conditions` into a query. `kv-conditions` are passed as a map of `column-name-keyword` -> `value`. The
  query compilation backend should merge the equivalent of `WHERE col = value` clauses into query, conjoined by
  `AND`."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ kv-conditions options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmulti rows*
  "Get the rows (e.g. HoneySQL `:values`) associated with an `insert-query`."
  {:arglists '([connectableᵈ tableableᵈ insert-queryᵈᵗ])}
  u/dispatch-on-first-three-args)

(m/defmulti with-rows*
  "Return a copy of `insert-query` with its rows replaced with `new-rows`."
  {:arglists '([connectableᵈ tableableᵈ insert-queryᵈᵗ new-rows options]), :style/indent :form}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmulti changes*
  "Return the updates (e.g. HoneySQL `:set`) that will be done to rows matching [[toucan2.build-query/conditions*]] for
  an `update-query`."
  {:arglists '([connectableᵈ tableableᵈ update-queryᵈᵗ])}
  u/dispatch-on-first-three-args)

(m/defmulti with-changes*
  "Return a copy of `update-query` with its changes that should be performed set to `new-changes`."
  {:arglists '([connectableᵈ tableableᵈ update-queryᵈᵗ new-changes options]), :style/indent :form}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

;; TODO FIXME
(defn new-query-dispatch-fn [connectable tableable _]
  [(u/dispatch-value connectable)
   (u/dispatch-value tableable)
   :toucan2/honeysql])

;; these dispatch of `connectable` and `tableable` indirectly.

(m/defmulti select-query*
  "Create a new empty select query against tableable."
  {:arglists '([connectableᵈ tableableᵈᵗ options])}
  new-query-dispatch-fn
  :combo (m.combo.threaded/threading-method-combination :second))

(m/defmulti update-query*
  {:arglists '([connectableᵈ tableableᵈᵗ options])}
  new-query-dispatch-fn
  :combo (m.combo.threaded/threading-method-combination :second))

(m/defmulti insert-query*
  {:arglists '([connectableᵈ tableableᵈᵗ options])}
  new-query-dispatch-fn
  :combo (m.combo.threaded/threading-method-combination :second))

(m/defmulti delete-query*
  {:arglists '([connectableᵈ tableableᵈᵗ options])}
  new-query-dispatch-fn
  :combo (m.combo.threaded/threading-method-combination :second))

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
          pk-map  (into {} (map
                            (fn [k v]
                              [k (compile/maybe-wrap-value connectable tableable k v options)])
                            pk-cols
                            pk-vals))]
      (merge kv-conditions pk-map))))
