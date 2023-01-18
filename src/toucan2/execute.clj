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
   [toucan2.pipeline :as pipeline]))

(set! *warn-on-reflection* true)

(defn- query* [f]
  (fn query**
    ([queryable]
     ;; `nil` connectable = use the current connection or `:default` if none is specified.
     (query** nil queryable))

    ([connectable queryable]
     (query** connectable nil queryable))

    ([connectable modelable queryable]
     ;; by passing `result-type/*` we'll basically get whatever the default is -- instances for `SELECT` or update counts
     ;; for `DML` stuff.
     (query** connectable :toucan.result-type/* modelable queryable))

    ([connectable query-type modelable queryable]
     (let [parsed-args {:connectable connectable
                        :modelable   modelable
                        :queryable   queryable}]
       (f query-type parsed-args)))))

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable]
                   [connectable query-type modelable queryable])}
  reducible-query
  "Create a reducible query that when reduced with resolve and compile `queryable`, open a connection using `connectable`
  and [[toucan2.connection/with-connection]], execute the query, and reduce the results.

  Note that this query can be something like a `SELECT` query, or something that doesn't normally return results, like
  an `UPDATE`; this works either way. Normally something like an `UPDATE` will reduce to the number of rows
  updated/inserted/deleted, e.g. `[5]`.

  You can specify `modelable` to execute the query in the context of a specific model; for queries returning rows, the
  rows will be returned as an [[toucan2.instance/instance]] of the resolved model.

  See [[toucan2.connection]] for Connection resolution rules."
  (query* pipeline/reducible-parsed-args))

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
  (query*
   (fn [query-type parsed-args]
     (let [rf (pipeline/default-rf query-type)]
       (pipeline/transduce-parsed rf query-type parsed-args)))))

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
  (query*
   (fn [query-type parsed-args]
     (let [rf    (pipeline/default-rf query-type)
           xform (pipeline/first-result-xform-fn query-type)]
       (pipeline/transduce-parsed (xform rf) query-type parsed-args)))))

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
