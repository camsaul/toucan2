(ns bluejdbc.metadata-fns
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.metadata :as metadata])
  (:import bluejdbc.result_set.ProxyResultSet
           java.sql.DatabaseMetaData))

(defn do-with-metadata
  "Impl for `with-metadata`."
  [connectable-or-metadata f options]
  (if (instance? DatabaseMetaData connectable-or-metadata)
    (f (metadata/proxy-database-metadata connectable-or-metadata options))
    (conn/with-connection [conn connectable-or-metadata]
      (f (metadata/metadata conn options)))))

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
  "Returns a `ResultSet` that when reduced or seqed returns catalog names available in this database.

    (into #{} (catalogs conn)) ; -> #{\"my_db\"}"
  ^ProxyResultSet [connectable-or-metadata & [options]]
  (with-metadata [metadata connectable-or-metadata (merge {:results/xform (constantly (map first))} options)]
    (.getCatalogs metadata)))

(defn schemas
  "Returns a `ResultSet` that when reduced or seqed returns maps of information about schemas in this database.

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
  ^ProxyResultSet [connectable-or-metadata & {:keys [^String catalog ^String schema-pattern options]}]
  (with-metadata [metadata connectable-or-metadata options]
    (.getSchemas metadata catalog schema-pattern)))

(defn table-types
  "Returns a `ResultSet` that when reduced or seqed returns a sequence of table types available in this database.

    (vec (table-types conn)) ;; -> [\"MATERIALIZED VIEW\" \"TABLE\" \"VIEW\"]"
  ^ProxyResultSet [connectable-or-metadata & [options]]
  (with-metadata [metadata connectable-or-metadata (merge {:results/xform (constantly (map first))} options)]
    (.getTableTypes metadata)))

(defn tables
  "Returns a `ResultSet` that when reduced or seqed returns maps of info about tables in this database.

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
  ^ProxyResultSet [connectable-or-metadata & {:keys [^String catalog
                                                     ^String schema-pattern
                                                     ^String table-name-pattern
                                                     types
                                                     options]
                                              :or   {types ["TABLE" "VIEW"]}}]
  (with-metadata [metadata connectable-or-metadata options]
    (.getTables metadata catalog schema-pattern table-name-pattern
                (when (seq types)
                  (into-array String (map str types))))))

(defn columns
  "Returns a `ResultSet` that when reduced or seqed returns maps of info about columns in this database.

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
  ^ProxyResultSet [connectable-or-metadata & {:keys [^String catalog
                                                     ^String schema-pattern
                                                     ^String table-name-pattern
                                                     ^String column-name-pattern
                                                     options]}]
  (with-metadata [metadata connectable-or-metadata options]
    (.getColumns metadata catalog schema-pattern table-name-pattern column-name-pattern)))
