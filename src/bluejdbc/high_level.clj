(ns bluejdbc.high-level
  "Higher-level functions."
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.options :as options]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util.log :as log]
            [pretty.core :as pretty])
  (:import java.sql.PreparedStatement))

(defn reducible-query
  "Return a reducible object that, when reduced, compiles (if needed) and runs `query-or-stmt`."
  {:arglists '([stmt] [stmt options] [conn query-or-stmt] [conn query-or-stmt options])}
  ([stmt]
   (stmt/proxy-prepared-statement stmt nil))

  ([x y]
   (if (instance? PreparedStatement x)
     (stmt/proxy-prepared-statement x y)
     (reducible-query x y nil)))

  ([connectable query-or-stmt options]
   (reify
     pretty/PrettyPrintable
     (pretty [_]
       (list 'reducible-query query-or-stmt))

     clojure.lang.IReduceInit
     (reduce [_ rf init]
       (conn/with-connection [conn connectable options]
         (stmt/with-prepared-statement [stmt conn query-or-stmt options]
           (reduce rf init stmt))))

     clojure.lang.IReduce
     (reduce [this rf]
       (reduce rf [] this)))))

(defn query
  "Execute a query, returning all results immediately. Non-lazy. `query-or-stmt` is one of:

    *  A SQL string
    *  A HoneySQL Form
    *  [sql & parameters] or [honeysql-form & parameters]
    *  A `PreparedStatement`
    *  Anything else that you've added that implements `bluejdbc.statement.CreatePreparedStatement`"
  {:arglists '([stmt] [stmt options] [connectable query-or-stmt] [connectable query-or-stmt options])}
  [& args]
  (reduce conj [] (apply reducible-query args)))

(defn- reduce-first [reducible]
  (transduce (take 1) (completing conj first) [] reducible))

(defn query-one
  "Like `query`, but only returns the first row. Other rows are not fetched."
  {:arglists '([stmt] [stmt options] [connectable query-or-stmt] [connectable query-or-stmt options])}
  ([stmt]
   (reduce-first (stmt/proxy-prepared-statement stmt {:stmt/max-rows 1})))

  ([x y]
   (if (instance? PreparedStatement x)
     (reduce-first (stmt/proxy-prepared-statement x (assoc y :stmt/max-rows 1)))
     (query-one x y nil)))

  ([connectable query-or-stmt options]
   (reduce-first (reducible-query connectable query-or-stmt (assoc options :stmt/max-rows 1)))))

(defn generated-keys [^PreparedStatement stmt]
  (log/trace "Fetching generated keys")
  (let [ks (:statement/return-generated-keys (options/options stmt))]
    (with-open [rs (.getGeneratedKeys stmt)]
      (let [result (into []
                         (cond
                           (keyword? ks)    (map ks)
                           (sequential? ks) (map #(select-keys % ks))
                           :else            identity)
                         rs)]
        result))))

(defn execute!
  "Execute a SQL *statement*, which can be SQL/HoneySQL/a `PreparedStatement`. The content of `query-or-stmt` itself
  must be either a DML statement such as `INSERT`, `UPDATE`, or `DELETE`, or a SQL statement that returns nothing,
  such as a DDL statement such as `CREATE TABLE`. Returns either the number of rows modified (for DML statements) or
  0 (for statements that return nothing)."
  {:arglists '(^Integer [stmt]
               ^Integer [stmt options]
               ^Integer [connectable query-or-stmt]
               ^Integer [connectable query-or-stmt options])}
  ([stmt]
   (execute! stmt nil))

  ([x y]
   (if (instance? PreparedStatement x)
     (let [[^PreparedStatement stmt options] [x y]
           affected-rows                     (.executeUpdate stmt)]
       (if (:statement/return-generated-keys (options/options stmt))
         (when (pos? affected-rows)
           (generated-keys stmt))
         affected-rows))
     (let [[connectable query-or-stmt] [x y]]
       (execute! connectable query-or-stmt nil))))

  ([connectable query-or-stmt options]
   (conn/with-connection [conn connectable options]
     (stmt/with-prepared-statement [stmt conn query-or-stmt options]
       (execute! stmt options)))))

(defn insert!
  "Convenience for inserting row(s). `rows` can either be maps or vectors. You should supply `columns` if rows are not
  maps.

    (insert! connectable :user [{:id 1, :name \"Cam\"} {:id 2, :name \"Sam\"}])

    (insert! connectable :user [:id :name] [[1 \"Cam\"] [2 \"Sam\"]]

  Returns number of rows inserted."
  {:arglists '(^Integer [connectable table-name row-or-rows]
               ^Integer [connectable table-name columns row-or-rows]
               ^Integer [connectable table-name row-or-rows options]
               ^Integer [connectable table-name columns row-or-rows options])}
  ([connectable table-name row-or-rows]
   (insert! connectable table-name nil row-or-rows nil))

  ([connectable table-name x y]
   (if (map? y)
     (insert! connectable table-name nil x   y)
     (insert! connectable table-name x   y nil)))

  ([connectable table-name columns row-or-rows options]
   (let [honeysql-form (merge {:insert-into table-name
                               :values      (if (map? row-or-rows)
                                              [row-or-rows]
                                              row-or-rows)}
                              (when (seq columns)
                                {:columns columns}))]
     (execute! connectable honeysql-form options))))

(defn insert-returning-keys!
  {:arglists '([connectable table-name row-or-rows]
               [connectable table-name columns row-or-rows]
               [connectable table-name row-or-rows options]
               [connectable table-name columns row-or-rows options])}
  [connectable & args]
  (conn/with-connection [conn connectable {:statement/return-generated-keys true}]
    (apply insert! conn args)))

(defn conditions->where-clause
  "Convert `conditions` to a HoneySQL-style WHERE clause vector if it is not already one (e.g., if it is a map)."
  [conditions]
  (cond
    (map? conditions)
    (let [clauses (for [[k v] conditions]
                    (if (vector? v)
                      (into [(first v) k] (rest v))
                      [:= k v]))]
      (if (> (bounded-count 2 clauses) 1)
        (into [:and] clauses)
        (first clauses)))

    (seq conditions)
    conditions))

(defn update!
  "Convenience for updating row(s). `conditions` can be either a map of `{field value}` or a HoneySQL-style vector where
  clause. Returns number of rows updated.

    ;; UPDATE venues SET expensive = false WHERE price = 1
    (jdbc/update! conn :venues {:price 1} {:expensive false})

    ;; Same as above, but with HoneySQL-style vector conditions
    (jdbc/update! conn :venues [:= :price 1] {:expensive false})

    ;; To use an operator other than `:=`, wrap the value in a vector e.g. `[:operator & values]`
    ;; UPDATE venues SET expensive = false WHERE price BETWEEN 1 AND 2
    (jdbc/update! conn :venues {:price [:between 1 2]} {:expensive false})"
  ([connectable table-name conditions changes]
   (update! connectable table-name conditions changes nil))

  ([connectable table-name conditions changes options]
   (let [honeysql-form (merge {:update table-name
                               :set    changes}
                              (when-let [where-clause (conditions->where-clause conditions)]
                                {:where where-clause}))]
     (execute! connectable honeysql-form options))))

(defn delete!
  "Convenience for deleting row(s). Args and options are the same as `update!` (excluding changes). Returns number of
  rows deleted."
  ([connectable table-name conditions]
   (delete! connectable table-name conditions nil))

  ([connectable table-name conditions options]
   (let [honeysql-form (merge {:delete-from table-name}
                              (when-let [where-clause (conditions->where-clause conditions)]
                                {:where where-clause}))]
     (execute! connectable honeysql-form options))))

;; TODO -- not sure this makes sense. what if we wanted to specify order/limit/etc??
(defn select
  ([connectable table-name]
   (select connectable table-name nil nil))

  ([connectable table-name conditions]
   (select connectable table-name conditions nil))

  ([connectable table-name conditions options]
   (let [honeysql-form (merge {:select [table-name]}
                              (when-let [where-clause (conditions->where-clause conditions)]
                                {:where where-clause}))]
     (query connectable honeysql-form options))))

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
