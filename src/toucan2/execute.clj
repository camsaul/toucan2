(ns toucan2.execute
  "Code for executing queries and statements, and reducing their results.

  The functions here meant for use on a regular basis are:

  * [[query]] -- resolve and compile a connectable, execute it using a connection from a connectable, and immediately
                 fully realize the results.

  * [[query-one]] -- like [[query]], but only realizes the first result.

  * [[reducible-query]] -- like [[query]], but returns a [[clojure.lang.IReduceInit]] that can be reduced later rather
    than immediately realizing the results.

  * [[with-call-count]] -- helper macro to count the number of queries executed within a `body`.

  #### Reducible query and pipeline.

  Basic execution pipeline is something this

  1. [[reduce-impl]]
     1. resolve `modelable` => `model` with [[toucan2.model/with-model]]
     2. resolve `queryable` => `query` with [[toucan2.query/with-resolved-query]]
  2. [[reduce-uncompiled-query]]
     1. Compile `query` => `compile-query` with [[toucan2.compile/with-compiled-query]]
  3. [[reduce-compiled-query]]
     1. Open `conn` from `connectable` with [[toucan2.connection/with-connection]]
  4. [[reduce-compiled-query-with-connection]]
     1. Execute and reduce the query with the open connection."
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.model :as model]
   [toucan2.protocols :as protocols]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(def ^:dynamic ^:private *call-count-thunk*
  "Thunk function to call every time a query is executed if [[with-call-count]] is in use."
  nil)

(m/defmulti reduce-compiled-query-with-connection
  "Execute a `compiled-query` with an open `connection` (by default a `java.sql.Connection`, but the actual connection
  type may be different depending on the connectable used to get it), and [[clojure.core/reduce]] the results using `rf`
  and `init`.

  The default implementation for `java.sql.Connection` is implemented by [[toucan2.jdbc.query/reduce-jdbc-query]].

  To support non-SQL databases you can provide your own implementation of this method;
  see [[toucan2.jdbc.query/reduce-jdbc-query]] for an example implementation.

  Dispatches off of `[conn model compiled-query]`."
  {:arglists '([conn model compiled-query rf init])}
  u/dispatch-on-first-three-args)

(m/defmethod reduce-compiled-query-with-connection :before :default
  [_conn _model _compiled-query _rf init]
  (when *call-count-thunk*
    (*call-count-thunk*))
  init)

(m/defmethod reduce-compiled-query-with-connection [java.sql.Connection :default clojure.lang.Sequential]
  [conn model sql-args rf init]
  (t2.jdbc.query/reduce-jdbc-query conn model sql-args rf init))

(m/defmulti reduce-compiled-query
  "Reduce a `compiled-query` with `rf` and `init`, presumably by obtaining a connection with `connectable`
  and [[toucan2.connection/with-connection]] and executing the query.

  The default implementation obtains a connection this way and then hands off
  to [[reduce-compiled-query-with-connection]].

  Normally you would never need to call or implement this yourself, but you can write a custom implementation if you
  want to do something tricky like skip query execution. See [[toucan2.tools.compile/compile]] for an example of doing
  this.

  Dispatches off of `[model compiled-query]`."
  {:arglists '([connectable model compiled-query rf init])}
  (fn [_connectable model compiled-query _rf _init]
    [(protocols/dispatch-value model) (protocols/dispatch-value compiled-query)]))

(m/defmethod reduce-compiled-query :default
  [connectable model compiled-query rf init]
  (let [connectable (or connectable
                        conn/*current-connectable*
                        (model/default-connectable model))]
    (conn/with-connection [conn connectable]
      (reduce-compiled-query-with-connection conn model compiled-query rf init))))

(m/defmulti reduce-uncompiled-query
  "Reduce an uncompiled `query` with `rf` and `init`.

  The default implementation of this compiles the query with [[compile/with-compiled-query]] and then hands off
  to [[reduce-compiled-query]], but you can write a custom implementation if you want to do something tricky like skip
  compilation to return a query directly. See [[toucan2.tools.identity-query]] for an example of doing this.

  Dispatches off of `[model query]`."
  {:arglists '([connectable model query rf init])}
  (fn [_connectable model query _rf _init]
    [(protocols/dispatch-value model) (protocols/dispatch-value query)]))

(m/defmethod reduce-uncompiled-query :default
  [connectable model query rf init]
  (compile/with-compiled-query [compiled-query [model query]]
    (reduce-compiled-query connectable model compiled-query rf init)))

(m/defmethod reduce-uncompiled-query :around :default
  [connectable model query rf init]
  ;; preserve the first uncompiled query we see if this gets called recursively
  (binding [u/*error-context* (merge {::uncompiled-query query} u/*error-context*)]
    (next-method connectable model query rf init)))

(defn- reduce-impl [connectable modelable queryable rf init]
  (model/with-model [model modelable]
    (query/with-resolved-query [query [model queryable]]
      (reduce-uncompiled-query connectable model query rf init))))

(deftype ^:no-doc ReducibleQuery [connectable modelable queryable]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce-impl connectable modelable queryable rf init))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable modelable queryable)))

(defn reducible-query
  "Create a reducible query that when reduced with resolve and compile `queryable`, open a connection using `connectable`
  and [[toucan2.connection/with-connection]], execute the query, and reduce the results.

  Note that this query can be something like a `SELECT` query, or something that doesn't normally return results, like
  an `UPDATE`; this works either way. Normally something like an `UPDATE` will reduce to the number of rows
  updated/inserted/deleted, e.g. `[5]`.

  You can specify `modelable` to execute the query in the context of a specific model; for queries returning rows, the
  rows will be returned as an [[toucan2.instance/instance]] of the resolved model.

  #### Connection Resolution

  If `connectable` is not specified, the query will be executed using a connection from the following options:

  1. the current connectable, [[toucan2.connection/*current-connectable*]], if bound;

  2. the [[toucan2.model/default-connectable]] for the model, if `modelable` was specified, and this is non-nil;

  3. the default connectable, `:default`, if it is specified by a `:default` method
     for [[toucan2.connection/do-with-connection]]

  The connection is not resolved until the query is executed, so you may create a reducible query with no default
  connection available and execute it later with one bound. (This also means that [[reducible-query]] does not capture
  dynamic bindings such as [[toucan2.connection/*current-connectable*]] -- you probably wouldn't want it to, anyway,
  since we have no guarantees and open connection will be around when we go to use the reducible query later.)"
  ([queryable]
   (reducible-query nil queryable))
  ([connectable queryable]
   (reducible-query connectable nil queryable))
  ([connectable modelable queryable]
   (->ReducibleQuery connectable modelable queryable)))

;;;; Util functions for running queries and immediately realizing the results.

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable])}
  query
  "Like [[reducible-query]], but immediately executes and fully reduces the query.

  ```clj
  (query ::my-connectable [\"SELECT * FROM venues;\"])
  ;; => [{:id 1, :name \"Tempest\"}
         {:id 2, :name \"BevMo\"}]
  ```

  Like [[reducible-query]], this may be used with either `SELECT` queries that return rows or things like `UPDATE` that
  normally return the count of rows updated."
  (comp realize/realize reducible-query))

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable])}
  query-one
  "Like [[query]], and immediately executes the query, but realizes and returns only the first result.

  ```clj
  (query-one ::my-connectable [\"SELECT * FROM venues;\"])
  ;; => {:id 1, :name \"Tempest\"}
  ```

  Like [[reducible-query]], this may be used with either `SELECT` queries that return rows or things like `UPDATE` that
  normally return the count of rows updated."
  (comp realize/reduce-first reducible-query))


;;;; [[with-call-count]]

(defn ^:no-doc do-with-call-counts
  "Impl for [[with-call-count]] macro; don't call this directly."
  [f]
  (let [call-count (atom 0)
        old-thunk  *call-count-thunk*]
    (binding [*call-count-thunk* (fn []
                                   (when old-thunk
                                     (old-thunk))
                                   (swap! call-count inc))]
      (f (fn [] @call-count)))))

(defmacro with-call-count
  "Execute `body`, trackingthe number of database queries and statements executed. This number can be fetched at any
  time withing `body` by calling function bound to `call-count-fn-binding`:

  ```clj
  (with-call-count [call-count]
    (select ...)
    (println \"CALLS:\" (call-count))
    (insert! ...)
    (println \"CALLS:\" (call-count)))
  ;; -> CALLS: 1
  ;; -> CALLS: 2
  ```"
  [[call-count-fn-binding] & body]
  `(do-with-call-counts (^:once fn* [~call-count-fn-binding] ~@body)))

;;; TODO -- this is kind of [[next.jdbc]] specific
(deftype ^:no-doc WithReturnKeys [reducible]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (binding [t2.jdbc.query/*options* (assoc t2.jdbc.query/*options* :return-keys true)]
      (reduce rf init reducible)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->WithReturnKeys reducible)))
