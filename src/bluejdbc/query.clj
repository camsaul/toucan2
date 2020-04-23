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

(defn execute!
  "Execute a SQL *statement*, which can be SQL/HoneySQL/a `PreparedStatement`. The content of `query-or-stmt` itself
  must be either a DML statement such as `INSERT`, `UPDATE`, or `DELETE`, or a SQL statement that returns nothing,
  such as a DDL statement such as `CREATE TABLE`. Returns either the number of rows modified (for DML statements) or
  0 (for statements that return nothing)."
  {:arglists '(^Integer [stmt] ^Integer [stmt options] ^Integer [connectable query-or-stmt] ^Integer [connectable query-or-stmt options])}
  ([stmt]
   (execute! stmt nil))

  ([x y]
   (if (instance? PreparedStatement x)
     (let [stmt (stmt/proxy-prepared-statement x y)]
       (.executeUpdate stmt))
     (execute! x y nil)))

  ([connectable query-or-stmt options]
   (conn/with-connection [conn connectable options]
     (stmt/with-prepared-statement [stmt conn query-or-stmt]
       (.executeUpdate stmt)))))

(defn insert!
  "Convenience for inserting rows. `rows` can either be maps or vectors. You should supply `columns` if rows are not
  maps.

    (insert! connectable :user [{:id 1, :name \"Cam\"} {:id 2, :name \"Sam\"}])

    (insert! connectable :user [:id :name] [[1 \"Cam\"] [2 \"Sam\"]]

  Returns number of rows inserted."
  {:arglists '(^Integer [connectable table-name rows]
               ^Integer [connectable table-name columns rows]
               ^Integer [connectable table-name rows options]
               ^Integer [connectable table-name columns rows options])}
  ([connectable table-name rows]
   (insert! connectable table-name nil rows nil))

  ([connectable table-name x y]
   (if (map? y)
     (insert! connectable table-name nil x   y)
     (insert! connectable table-name x   y nil)))

  ([connectable table-name columns rows options]
   (let [honeysql-form (merge {:insert-into table-name
                               :values      rows}
                              (when (seq columns)
                                {:columns columns}))]
     (execute! connectable honeysql-form options))))
