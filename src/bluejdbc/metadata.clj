(ns bluejdbc.metadata
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.result-set :as rs])
  (:import java.sql.DatabaseMetaData))

(defn metadata-reducible-results
  "Helper to return a reducible/seqable metadata results with user-friendly map key transformations."
  (^clojure.lang.IReduceInit [rs]
   (metadata-reducible-results rs nil))

  (^clojure.lang.IReduceInit [rs options]
   (rs/reducible-result-set rs (merge {:results/xform (rs/maps :lower-case :lisp-case)} options))))

(defn do-with-metadata
  "Impl for `with-metadata`."
  [connectable-or-metadata f options]
  (if (instance? DatabaseMetaData connectable-or-metadata)
    (f connectable-or-metadata)
    (conn/with-connection [conn connectable-or-metadata]
      (f (.getMetaData conn)))))

(defmacro with-metadata
  "Execute `body` with `metadata-binding` bound to a `DatabaseMetaData`"
  [[metadata-binding connectable-or-metadata & [options]] & body]
  `(do-with-metadata ~connectable-or-metadata
                     (fn [~(vary-meta metadata-binding assoc :tag 'java.sql.DatabaseMetaData)]
                       ~@body)
                     ~options))

(defn database-info
  "Fetch a map of information about this database."
  [connectable-or-metadata]
  (with-metadata [metadata connectable-or-metadata]
    {:name          (.getDatabaseProductName metadata)
     :version       (.getDatabaseProductVersion metadata)
     :major-version (.getDatabaseMajorVersion metadata)
     :minor-version (.getDatabaseMinorVersion metadata)}))

(defn driver-info
  "Fetch a map of information about the JDBC driver used for this database."
  [connectable-or-metadata]
  (with-metadata [metadata connectable-or-metadata]
    {:name          (.getDriverName metadata)
     :version       (.getDriverVersion metadata)
     :major-version (.getDriverMajorVersion metadata)
     :minor-version (.getDriverMinorVersion metadata)}))

(defn catalogs
  "Returns a `clojure.lang.IReduceInit` that when reduced or seqed returns catalog names available in this database.

    (into #{} (catalogs conn)) ; -> #{\"my_db\"}"
  [connectable-or-metadata & [options]]
  (with-metadata [metadata connectable-or-metadata (merge {:results/xform (constantly (map first))} options)]
    (metadata-reducible-results
     (.getCatalogs metadata)
     options)))

(defn schemas
  "Returns a `clojure.lang.IReduceInit` that when reduced or seqed returns maps of information about schemas in this
  database.

    (vec (schemas conn))
    ;; ->
    [{:table-schem \"information_schema\", :table-catalog nil}
     {:table-schem \"pg_catalog\", :table-catalog nil}
     {:table-schem \"public\", :table-catalog nil}]

  Options:

  * `:catalog` - a catalog name; must match the catalog name as it is stored in the database; \"\" retrieves those
    without a catalog; `nil` means catalog name should not be used to narrow down the search.

  * `:schema-pattern` - a schema name; must match the schema name as it is stored in the database; `nil` means schema
    name should not be used to narrow down the search"
  [connectable-or-metadata & {:keys [^String catalog ^String schema-pattern options]}]
  (with-metadata [metadata connectable-or-metadata options]
    (metadata-reducible-results
     (.getSchemas metadata catalog schema-pattern)
     options)))

(defn table-types
  "Returns a `clojure.lang.IReduceInit` that when reduced or seqed returns a sequence of table types available in this
  database.

    (vec (table-types conn)) ;; -> [\"MATERIALIZED VIEW\" \"TABLE\" \"VIEW\"]"
  [connectable-or-metadata & [options]]
  (with-metadata [metadata connectable-or-metadata (merge {:results/xform (constantly (map first))} options)]
    (metadata-reducible-results
     (.getTableTypes metadata)
     options)))

(defn tables
  "Returns a `clojure.lang.IReduceInit` that when reduced or seqed returns maps of info about tables in this database.

    (take 2 (tables conn))
    ;; ->
    [{:table-schem \"public\", :table-name \"users\", ...}
     ...]

  Options:

  * `:catalog` - a catalog name; must match the catalog name as it is stored in the database; \"\" retrieves those
    without a catalog; `nil` means catalog name should not be used to narrow down the search.

  * `:schema-pattern` - a schema name; must match the schema name as it is stored in the database; `nil` means schema
    name should not be used to narrow down the search

  * `:table-name-pattern` - a table name pattern; must match the table name as it is stored in the database. `nil`
    means table name should not be used to narrow down the search

  *  `:types` -- table type name strings, such as `\"TABLE\"` or `\"VIEW\"`. Use `table-types` to get the available
     types for the current database. By default, Blue JDBC restricts types to `TABLE` and `VIEW`, which is more useful
     for exploratory REPL usage. You can pass `nil` to return all types."
  [connectable-or-metadata & {:keys [^String catalog
                                     ^String schema-pattern
                                     ^String table-name-pattern
                                     types
                                     options]
                              :or   {types ["TABLE" "VIEW"]}}]
  (with-metadata [metadata connectable-or-metadata options]
    (metadata-reducible-results
     (.getTables metadata catalog schema-pattern table-name-pattern
                 (when (seq types)
                   (into-array String (map str types))))
     options)))

(defn columns
  "Returns a `clojure.lang.IReduceInit` that when reduced or seqed returns maps of info about columns in this database.

    (into #{} (map :column-name (columns conn :table-name-pattern \"users\")))
    ;; -> #{\"last_login\" \"id\" \"name\" \"password\"}

  Options:

  * `:catalog` - a catalog name; must match the catalog name as it is stored in the database; \"\" retrieves those
    without a catalog; `nil` means catalog name should not be used to narrow down the search.

  * `:schema-pattern` - a schema name; must match the schema name as it is stored in the database; `nil` means schema
    name should not be used to narrow down the search

  * `:table-name-pattern` - a table name pattern; must match the table name as it is stored in the database. `nil`
    means table name should not be used to narrow down the search

  *  `:column-name-pattern` - a column name pattern; must match the column name as it is stored in the database. `nil`
     means column name should not be used to narrow down the search."
  [connectable-or-metadata & {:keys [^String catalog
                                     ^String schema-pattern
                                     ^String table-name-pattern
                                     ^String column-name-pattern
                                     options]}]
  (with-metadata [metadata connectable-or-metadata options]
    (metadata-reducible-results
     (.getColumns metadata catalog schema-pattern table-name-pattern column-name-pattern)
     options)))
