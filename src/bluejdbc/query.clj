(ns bluejdbc.query
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [pretty.core :as pretty])
  (:import java.sql.PreparedStatement))

(def ^:dynamic *include-queries-in-exceptions* false) ; EXPERIMENTAL

(defmacro try-catch-when-include-queries-in-exceptions-enabled
  {:style/indent 0}
  [& args]
  (let [body       (butlast args)
        catch-form (last args)]
    `(let [thunk# (fn [] ~@body)]
       (if *include-queries-in-exceptions*
         (try
           (thunk#)
           ~catch-form)
         (thunk#)))))

(m/defmulti reducible-query*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-query* :default
  [connectable query options]
  (reify
    pretty/PrettyPrintable
    (pretty [_]
      (list 'bluejdbc.query/reducible-query query))

    clojure.lang.IReduceInit
    (reduce [_ rf init]
      (conn/with-connection [conn connectable options]
        (let [sql-args (compile/compile conn query options)]
          (try-catch-when-include-queries-in-exceptions-enabled
            (with-open [stmt (stmt/prepare! conn sql-args options)
                        rs   (.executeQuery stmt)]
              (try
                (rs/reduce-result-set rs options rf init)
                (catch Throwable e
                  (throw (ex-info "Error reducing results"
                                  {:result-set rs, :rf rf, :init init, :options options}
                                  e)))))
            (catch Throwable e
              (throw (ex-info "Error executing query"
                              {:query query, :sql-args sql-args, :options options}
                              e)))))))

    clojure.lang.IReduce
    (reduce [this rf]
      (reduce rf [] this))))

(defn reducible-query
  ([query]                     (reducible-query* :default    query nil))
  ([connectable query]         (reducible-query* connectable query nil))
  ([connectable query options] (reducible-query* connectable query options)))

(m/defmulti query*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(m/defmethod query* :default
  [connectable query options]
  (reduce conj [] (reducible-query* connectable query options)))

(defn query
  ([query]                     (query* :default    query nil))
  ([connectable query]         (query* connectable query nil))
  ([connectable query options] (query* connectable query options)))

(m/defmulti query-one*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(defn- reduce-first [reducible]
  (transduce (take 1) (completing conj first) [] reducible))

(m/defmethod query-one* :default
  [connectable query options]
  (reduce-first (reducible-query* connectable query options)))

(defn query-one
  ([query]                     (query-one* :default    query nil))
  ([connectable query]         (query-one* connectable query nil))
  ([connectable query options] (query-one* connectable query options)))

(defn generated-keys
  "Fetch the generated keys created after executing `stmt` (i.e., an `INSERT` statement). Results are a sequence of one
  result per row, but the format of each row depends on the `:statement/return-generated-keys` option itself: a truthy
  value returns everything; a keyword column name returns just that column; a sequence of column names returns a
  sequence of maps containing those keys.

    ;; :statement/return-generated-keys -> :id
    (generated-keys stmt) -> [1 2 3]"
  [^PreparedStatement stmt options]
  (log/trace "Fetching generated keys")
  (try
    (let [ks (:statement/return-generated-keys options)]
      (try
        (with-open [rs (.getGeneratedKeys stmt)]
          (let [result (into []
                             (cond
                               (keyword? ks)    (map ks)
                               (sequential? ks) (map #(select-keys % ks))
                               :else            identity)
                             (rs/reducible-result-set rs options))]
            result))
        (catch Throwable e
          (throw (ex-info "Error fetching generated keys"
                          {:keys ks, :options options}
                          e)))))))

(m/defmulti execute!*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(m/defmethod execute!* :default
  [connectable query options]
  (conn/with-connection [conn connectable options]
    (let [sql-args (compile/compile conn query options)]
      (try-catch-when-include-queries-in-exceptions-enabled
        (with-open [stmt (stmt/prepare! conn sql-args options)]
          (let [affected-rows (.executeUpdate stmt)]
            (if (:statement/return-generated-keys options)
              (when (pos? affected-rows)
                (generated-keys stmt options))
              affected-rows)))
        (catch Throwable e
          (throw (ex-info "Error executing statement"
                          {:query query, :sql-args sql-args, :options options}
                          e)))))))

(defn execute!
  ([query]                     (execute!* :default    query nil))
  ([connectable query]         (execute!* connectable query nil))
  ([connectable query options] (execute!* connectable query options)))

(defn do-transaction
  "Impl for `transaction` macro."
  [connectable f]
  (conn/with-connection [conn connectable]
    (log/trace "Begin transaction")
    (let [orig-auto-commit (.getAutoCommit conn)]
      ;; use two `try` blocks here so if `.setSavepoint` throws an Exception the original value of auto-commit is
      ;; restored
      (try
        (.setAutoCommit conn false)
        (let [savepoint (.setSavepoint conn)]
          (try
            (f conn)
            (log/trace "Commit transaction")
            (.commit conn)
            (catch Throwable e
              (log/trace "Rolling back transaction")
              (.rollback conn savepoint)
              (throw e))))
        (finally
          (.setAutoCommit conn orig-auto-commit))))))

(defmacro transaction
  "Execute `body` inside a JDBC transaction. Transaction is committed if body completes successfully; if body throws an
  Exception, transaction is aborted.

  `transaction` can be used with anything connectable, and binding the resulting connection is optional:

    ;; option 1
    (transaction [conn my-datasource]
      ...)

    ;; option 2
    (jdbc/with-connection [conn my-datasource]
      (transaction conn
        ...))"
  {:style/indent 1, :arglists '([connectable & body] [[conn-binding connectable] & body])}
  [arg & body]
  (if (and (sequential? arg)
           (symbol? (first arg)))
    (let [[conn-binding connectable] arg]
      `(do-transaction ~connectable (fn [~conn-binding] ~@body)))
    (let [connectable arg]
      `(do-transaction ~connectable (fn [_conn#] ~@body)))))
