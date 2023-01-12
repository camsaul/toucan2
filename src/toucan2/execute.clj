(ns toucan2.execute
  "Code for executing queries and statements, and reducing their results.

  The functions here meant for use on a regular basis are:

  * [[query]] -- resolve and compile a connectable, execute it using a connection from a connectable, and immediately
                 fully realize the results.

  * [[query-one]] -- like [[query]], but only realizes the first result.

  * [[reducible-query]] -- like [[query]], but returns a [[clojure.lang.IReduceInit]] that can be reduced later rather
    than immediately realizing the results.

  * [[with-call-count]] -- helper macro to count the number of queries executed within a `body`."
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]))

(set! *warn-on-reflection* true)

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
   ;; `nil` connectable = use the current connection or `:default` if none is specified.
   (reducible-query nil queryable))

  ([connectable queryable]
   (reducible-query connectable nil queryable))

  ([connectable modelable queryable]
   ;; by passing `result-type/*` we'll basically get whatever the default is -- instances for `SELECT` or update counts
   ;; for `DML` stuff.
   (reducible-query connectable :toucan.result-type/* modelable queryable))

  ([connectable query-type modelable queryable]
   (reify clojure.lang.IReduceInit
     (reduce [_this rf init]
       (reduce rf
               init
               (pipeline/reducible-parsed-args
                query-type
                {:connectable connectable
                 :modelable   modelable
                 :queryable   queryable}))))))

;;;; Util functions for running queries and immediately realizing the results.

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable]
                   [connectable query-type modelable queryable])}
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
                   [connectable modelable queryable]
                   [connectable query-type modelable queryable])}
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
        old-thunk  pipeline/*call-count-thunk*]
    (binding [pipeline/*call-count-thunk* (fn []
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

(s/fdef with-call-count
  :args (s/cat :bindings (s/spec (s/cat :call-count-binding symbol?))
               :body     (s/+ any?))
  :ret any?)
