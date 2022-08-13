(ns toucan2.query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]
   [toucan2.model :as model]))

(m/defmulti reduce-query-with-connection
  "Reduce `compiled-query` with an opened `connection`."
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-first-two-args)

;; TODO -- should this live in [[t2.jdbc.query]] instead?
(m/defmethod reduce-query-with-connection [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (t2.jdbc.query/reduce-jdbc-query conn sql-args rf init))

(m/defmulti reduce-query
  "Reduce `compiled-query`. `connectable` has not been realized yet. Normally this hands off
  to [[reduce-query-with-connection]]."
  {:arglists '([connectable compiled-query rf init])}
  u/dispatch-on-second-arg)

(m/defmethod reduce-query :default
  [connectable compiled-query rf init]
  (conn/with-connection [conn connectable]
    (try
      (reduce-query-with-connection conn compiled-query rf init)
      (catch Throwable e
        (throw (ex-info (format "Error reducing query: %s" (ex-message e))
                        {:query compiled-query, :rf rf, :init init}
                        e))))))

(defrecord ReducibleQuery [connectable queryable]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (compile/with-compiled-query [compiled-query queryable]
      (reduce-query connectable compiled-query rf init)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable queryable)))

(defn reducible-query
  ([queryable]
   (reducible-query current/*connection* queryable))
  ([connectable queryable]
   (->ReducibleQuery connectable queryable)))

(defn query
  ([queryable]
   (query current/*connection* queryable))
  ([connectable queryable]
   (realize/realize (reducible-query connectable queryable))))

(defn query-one
  "Like [[query]], but returns only the first row. Does not fetch additional rows regardless of whether the query would
  have yielded them."
  ([queryable]
   (query-one current/*connection* queryable))
  ([connectable queryable]
   (realize/reduce-first (reducible-query connectable queryable))))

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
  ([queryable]
   (execute! current/*connection* queryable))

  ([connectable queryable]
   (compile/with-compiled-query [compiled-query queryable]
     (conn/with-connection [conn connectable]
       (try
         (execute!* conn compiled-query)
         (catch Throwable e
           (throw (ex-info (format "Error executing query: %s" (ex-message e))
                           {:query compiled-query}
                           e))))))))

(defrecord ReducibleQueryAs [connectable modelable queryable]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (model/with-model [_model modelable]
      (reduce rf init (reducible-query connectable queryable))
      #_(binding [query/*jdbc-options* (merge
                                        {:builder-fn (instance/instance-result-set-builder model)}
                                        query/*jdbc-options*)]
          (reduce rf init (query/reducible-query connectable query)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query-as connectable modelable query)))

(defn reducible-query-as
  ([modelable queryable]
   (reducible-query-as :toucan/current-connectable-for-current-model modelable queryable))
  ([connectable modelable queryable]
   (->ReducibleQueryAs connectable modelable queryable)))

;;; TODO -- should this only have a three arity, like [[reducible-query-as]] does?
(defn query-as
  ([modelable queryable]
   (query-as :toucan/current-connectable-for-current-model modelable queryable))
  ([connectable modelable queryable]
   (realize/realize (reducible-query-as connectable modelable queryable))))

;;; TODO

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
