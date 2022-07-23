(ns toucan2.query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(m/defmulti reduce-query
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-first-two-args)

;; TODO -- should this live in [[t2.jdbc.query]] instead?
(m/defmethod reduce-query [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (t2.jdbc.query/reduce-jdbc-query conn sql-args rf init))

(defrecord ReducibleQuery [connectable query]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (conn/with-connection [conn connectable]
      (compile/with-compiled-query [compiled-query [conn query]]
        (reduce-query conn compiled-query rf init))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable query)))

(defn reducible-query
  ([a-query]
   (reducible-query current/*connection* a-query))
  ([connectable a-query]
   (->ReducibleQuery connectable a-query)))

(defn query
  ([a-query]
   (query current/*connection* a-query))
  ([connectable a-query]
   (realize/realize (reducible-query connectable a-query))))

(defn query-one
  "Like `query`, but returns only the first row. Does not fetch additional rows regardless of whether the query would
  yielded them."
  ([a-query]
   (query-one current/*connection* a-query))
  ([connectable a-query]
   (realize/reduce-first (reducible-query connectable a-query))))

(m/defmulti execute!*
  {:arglists '([connection compiled-query])}
  u/dispatch-on-first-two-args)

(m/defmethod execute!* [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args]
  (t2.jdbc.query/execute-jdbc-query! conn sql-args))

(defn execute!
  "Compile and execute a `query` such as a String SQL statement, `[sql & params]` vector, or HoneySQL map. Intended for
  use with statements such as `UPDATE`, `INSERT`, or `DELETE`, or DDL statements like `CREATE TABLE`; for queries like
  `SELECT`, use [[query]] instead."
  ([query]
   (execute! current/*connection* query))

  ([connectable query]
   (conn/with-connection [conn connectable]
     (compile/with-compiled-query [compiled-query [conn query]]
       (execute!* conn compiled-query)))))

;; TODO

#_(defn do-with-call-counts
    "Impl for [[with-call-count]] macro; don't call this directly."
    [f]
    (let [call-count (atom 0)
          old-thunk  *call-count-thunk*]
      (binding [*call-count-thunk* #(do
                                      (old-thunk)
                                      (swap! call-count inc))]
        (f (fn [] @call-count)))))

#_(defmacro with-call-count
  "Execute `body`, trackingthe number of database queries and statements executed. This number can be fetched at any
  time withing `body` by calling function bound to `call-count-fn-binding`:

    (with-call-count [call-count]
      (select ...)
      (println \"CALLS:\" (call-count))
      (insert! ...)
      (println \"CALLS:\" (call-count)))
    ;; -> CALLS: 1
    ;; -> CALLS: 2"
  [[call-count-fn-binding] & body]
  `(do-with-call-counts (fn [~call-count-fn-binding] ~@body)))
