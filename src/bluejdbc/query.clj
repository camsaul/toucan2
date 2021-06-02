(ns bluejdbc.query
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.log :as log]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.result-set :as next.jdbc.rs]
            [potemkin :as p]
            [pretty.core :as pretty]))

;; TODO -- should this be off by default?
(def ^:dynamic *include-queries-in-exceptions?* true)

(def ^:dynamic ^:private *call-count-thunk* (fn [])) ; no-op

(def ^:dynamic ^:private *return-compiled* false)

(def ^:dynamic ^:private *return-uncompiled* false)

(defn do-compiled [thunk]
  (binding [*return-compiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro compiled {:style/indent 0} [& body]
  `(do-compiled (fn [] ~@body)))

(defn do-uncompiled [thunk]
  (binding [*return-uncompiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro uncompiled {:style/indent 0} [& body]
  `(do-uncompiled (fn [] ~@body)))

(defn- compile [connectable tableable queryable options]
  (when *return-uncompiled*
    (throw (u/quit-early-exception queryable)))
  (let [sql-params (compile/compile connectable tableable queryable options)]
    (when *return-compiled*
      (throw (u/quit-early-exception sql-params)))
    sql-params))

(defn- reduce-query
  [connectable tableable queryable options rf init]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        sql-params            (compile connectable tableable queryable options)]
    (conn/with-connection [conn connectable tableable options]
      (try
        (log/with-trace ["Executing query %s with options %s" sql-params (:next.jdbc options)]
          (let [[sql & params] sql-params
                params         (for [param params]
                                 (stmt/parameter connectable tableable param options))
                sql-params     (cons sql params)
                results        (next.jdbc/plan conn sql-params (:next.jdbc options))]
            (try
              (log/with-trace ["Reducing results with rf %s and init %s" rf init]
                (*call-count-thunk*)
                (reduce rf init results))
              (catch Throwable e
                (let [message (or (:message (ex-data e)) (ex-message e))]
                  (throw (ex-info (format "Error reducing results: %s" message)
                                  {:rf rf, :init init, :message message}
                                  e)))))))
        (catch Throwable e
          (let [message (or (:message (ex-data e)) (ex-message e))]
            (throw (ex-info (format "Error executing query: %s" message)
                            (merge
                             {:options options
                              :message message}
                             (when *include-queries-in-exceptions?*
                               {:query queryable, :sql-params sql-params, :options options}))
                            e))))))))

(declare all)

(p/deftype+ ReducibleQuery [connectable tableable queryable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list (u/qualify-symbol-for-*ns* `reducible-query) connectable tableable queryable options))

  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce-query connectable tableable queryable options rf init)))

(m/defmulti reducible-query*
  {:arglists '([connectable tableable queryable options])}
  u/dispatch-on-first-three-args)

(m/defmethod reducible-query* :default
  [connectable tableable queryable options]
  (assert (some? connectable) "connectable should not be nil; use current-connectable to get the connectable to use")
  (->ReducibleQuery connectable tableable queryable options))

(defn reducible-query
  ([queryable]
   (reducible-query nil nil queryable nil))

  ([connectable queryable]
   (reducible-query connectable nil queryable nil))

  ([connectable tableable queryable]
   (reducible-query connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (reducible-query* connectable tableable queryable options))))

(defn realize-row [row]
  (next.jdbc.rs/datafiable-row row conn.current/*current-connection* nil))

(defn all [reducible-query]
  (into
   []
   (map realize-row)
   reducible-query))

(defn query
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (all (apply reducible-query args)))

(defn reduce-first
  ([reducible]
   (reduce-first identity reducible))

  ([xform reducible]
   (transduce
    (comp xform (take 1))
    (completing conj first)
    []
    reducible)))

(defn query-one
  "Like `query`, but returns only the first row. Does not fetch additional rows regardless of whether the query would
  yielded them."
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (reduce-first (map realize-row) (apply reducible-query args)))

(m/defmulti execute!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod execute!* :default
  [connectable tableable query options]
  (conn/with-connection [conn connectable tableable options]
    (let [sql-params (compile connectable tableable query options)]
      (try
        (*call-count-thunk*)
        (let [results (next.jdbc/execute! conn sql-params (:next.jdbc options))]
          (if (get-in options [:next.jdbc :return-keys])
            results
            (-> results first :next.jdbc/update-count)))
        (catch Throwable e
          (throw (ex-info (format "Error executing statement: %s" (ex-message e))
                          (merge
                           {:options options}
                           (when *include-queries-in-exceptions?*
                             {:query query, :sql-params sql-params}))
                          e)))))))

(defn execute!
  "Compile and execute a `queryable` such as a String SQL statement, `[sql & params]` vector, or HoneySQL map. Intended
  for use with statements such as `UPDATE`, `INSERT`, or `DELETE`, or DDL statements like `CREATE TABLE`; for queries
  like `SELECT`, use `query` instead. Returns the number of rows affected, unless `{:next.jdbc {:return-keys true}}`
  is set, in which case it returns generated keys (see next.jdbc documentation for more details)."
  ([queryable]                       (execute!  nil         nil       queryable nil))
  ([connectable queryable]           (execute!  connectable nil       queryable nil))
  ([connectable tableable queryable] (execute!  connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (execute!* connectable tableable queryable options))))

(defn do-with-call-counts
  "Impl for `with-call-count` macro; don't call this directly."
  [f]
  (let [call-count (atom 0)
        old-thunk  *call-count-thunk*]
    (binding [*call-count-thunk* #(do
                                    (old-thunk)
                                    (swap! call-count inc))]
      (f (fn [] @call-count)))))

(defmacro with-call-count
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
