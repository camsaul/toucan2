(ns bluejdbc.query
  "Higher-level functions."
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.statement :as stmt]
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

  ([conn query-or-stmt options]
   (reify
     pretty/PrettyPrintable
     (pretty [_]
       (list 'reducible-query query-or-stmt))

     clojure.lang.IReduceInit
     (reduce [_ rf init]
       (conn/with-connection [conn conn options]
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
  {:arglists '([stmt] [stmt options] [conn query-or-stmt] [conn query-or-stmt options])}
  [& args]
  (reduce conj [] (apply reducible-query args)))

(defn- reduce-first [reducible]
  (transduce (take 1) (completing conj first) [] reducible))

(defn query-one
  "Like `query`, but only returns the first row. Other rows are not fetched."
  {:arglists '([stmt] [stmt options] [conn query-or-stmt] [conn query-or-stmt options])}
  ([stmt]
   (reduce-first (stmt/proxy-prepared-statement stmt {:stmt/max-rows 1})))

  ([x y]
   (if (instance? PreparedStatement x)
     (reduce-first (stmt/proxy-prepared-statement x (assoc y :stmt/max-rows 1)))
     (query-one x y nil)))

  ([conn query-or-stmt options]
   (reduce-first (reducible-query conn query-or-stmt (assoc options :stmt/max-rows 1)))))

(defn execute!
  "Execute a SQL *statement*, which can be SQL/HoneySQL/a `PreparedStatement`. The content of `query-or-stmt` itself
  must be either a DML statement such as `INSERT`, `UPDATE`, or `DELETE`, or a SQL statement that returns nothing,
  such as a DDL statement such as `CREATE TABLE`. Returns either the number of rows modified (for DML statements) or
  0 (for statements that return nothing)."
  {:arglists '(^Integer [stmt] ^Integer [stmt options] ^Integer [conn query-or-stmt] ^Integer [conn query-or-stmt options])}
  ([stmt]
   (execute! stmt nil))

  ([x y]
   (if (instance? PreparedStatement x)
     (let [stmt (stmt/proxy-prepared-statement x y)]
       (.executeUpdate stmt))
     (execute! x y nil)))

  ([conn query-or-stmt options]
   (conn/with-connection [conn conn options]
     (stmt/with-prepared-statement [stmt conn query-or-stmt]
       (.executeUpdate stmt)))))

(defn insert!
  "Convenience for inserting rows. `rows` can either be maps or vectors. You should supply `columns` if rows are not
  maps.

    (insert! conn :user [{:id 1, :name \"Cam\"} {:id 2, :name \"Sam\"}])

    (insert! conn :user [:id :name] [[1 \"Cam\"] [2 \"Sam\"]]

  Returns number of rows inserted."
  {:arglists '(^Integer [conn table-name columns? rows options?])}
  ([conn table-name rows]
   (insert! conn table-name nil rows nil))

  ([conn table-name x y]
   (if (map? y)
     (insert! conn table-name nil x   y)
     (insert! conn table-name x   y nil)))

  ([conn table-name columns rows options]
   (let [honeysql-form (merge {:insert-into table-name
                               :values      rows}
                              (when (seq columns)
                                {:columns columns}))]
     (execute! conn honeysql-form options))))
