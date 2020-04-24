(ns bluejdbc.metadata
  (:require [bluejdbc.options :as options]
            [bluejdbc.result-set :as rs]
            [bluejdbc.util :as u]
            [pretty.core :as pretty])
  (:import bluejdbc.result_set.ProxyResultSet
           [java.sql Connection DatabaseMetaData ResultSet]))

(defn metadata-result-set
  "Helper to return a reducible/seqable `ResultSet` with user-friend map key transformations."
  ^ProxyResultSet [rs & [options]]
  (rs/reducible-result-set rs (merge {:results/xform (rs/maps :lower-case :lisp-case)} options)))

(u/define-proxy-class ProxyDatabaseMetaData DatabaseMetaData [dbmeta mta opts]
  pretty/PrettyPrintable
  (pretty [_]
    (list 'proxy-database-metadata dbmeta opts))

  options/Options
  (options [_]
    opts)

  (with-options* [_ new-options]
    (ProxyDatabaseMetaData. dbmeta mta new-options))

  clojure.lang.IObj
  (meta [_]
    mta)

  (withMeta [_ new-meta]
    (ProxyDatabaseMetaData. dbmeta new-meta opts))

  DatabaseMetaData
  (^ResultSet getAttributes [_ ^String catalog ^String schema-pattern ^String type-name-pattern ^String attribute-name-pattern]
   (metadata-result-set (.getAttributes dbmeta catalog schema-pattern type-name-pattern attribute-name-pattern) opts))

  (^ResultSet getBestRowIdentifier
   [_ ^String catalog ^String schema ^String table ^int scope ^boolean nullable]
   (metadata-result-set (.getBestRowIdentifier dbmeta catalog schema table scope nullable) opts))

  (^ResultSet getCatalogs [_]
   (metadata-result-set (.getCatalogs dbmeta) opts))

  (^ResultSet getClientInfoProperties [_]
   (metadata-result-set (.getClientInfoProperties dbmeta) opts))

  (^ResultSet getColumnPrivileges [_ ^String catalog ^String schema ^String table ^String column-name-pattern]
   (metadata-result-set (.getColumnPrivileges dbmeta catalog schema table column-name-pattern) opts))

  (^ResultSet getColumns [_ ^String catalog ^String schema ^String table ^String column-name-pattern]
   (metadata-result-set (.getColumns dbmeta catalog schema table column-name-pattern) opts))

  (^java.sql.Connection getConnection [_]
   (or (:_connection opts)
       (.getConnection dbmeta)))

  (^ResultSet getExportedKeys [_ ^String catalog ^String schema ^String table]
   (metadata-result-set (.getExportedKeys dbmeta catalog schema table) opts))

  (^ResultSet getFunctionColumns [_ ^String catalog ^String schema-pattern ^String function-name-pattern ^String column-name-pattern]
   (metadata-result-set (.getFunctionColumns dbmeta catalog schema-pattern function-name-pattern column-name-pattern) opts))

  (^ResultSet getFunctions [_ ^String catalog ^String schema-pattern ^String function-name-pattern]
   (metadata-result-set (.getFunctions dbmeta catalog schema-pattern function-name-pattern) opts))

  (^ResultSet getImportedKeys [_ ^String catalog ^String schema ^String table]
   (metadata-result-set (.getImportedKeys dbmeta catalog schema table) opts))

  (^ResultSet getIndexInfo [_ ^String catalog ^String schema ^String table ^boolean unique ^boolean approximate]
   (metadata-result-set (.getIndexInfo dbmeta catalog schema table unique approximate) opts))

  (^ResultSet getPrimaryKeys [_ ^String catalog ^String schema ^String table]
   (metadata-result-set (.getPrimaryKeys dbmeta catalog schema table) opts))

  (^ResultSet getProcedureColumns [_ ^String catalog ^String schema-pattern ^String procedure-name-pattern ^String column-name-pattern]
   (metadata-result-set (.getProcedureColumns dbmeta catalog schema-pattern procedure-name-pattern column-name-pattern) opts))

  (^ResultSet getProcedures [_ ^String catalog ^String schema-pattern ^String procedure-name-pattern]
   (metadata-result-set (.getProcedures dbmeta catalog schema-pattern procedure-name-pattern) opts))

  (^ResultSet getPseudoColumns [_ ^String catalog ^String schema-pattern ^String table-name-pattern ^String column-name-pattern]
   (metadata-result-set (.getPseudoColumns dbmeta catalog schema-pattern table-name-pattern column-name-pattern) opts))

  (^ResultSet getSchemas [_]
   (metadata-result-set (.getSchemas dbmeta) opts))

  (^ResultSet getSchemas [_ ^String catalog ^String schema-pattern]
   (metadata-result-set (.getSchemas dbmeta catalog schema-pattern) opts))

  (^ResultSet getSuperTables [_ ^String catalog ^String schema-pattern ^String table-name-pattern]
   (metadata-result-set (.getSuperTables dbmeta catalog schema-pattern table-name-pattern) opts))

  (^ResultSet getSuperTypes [_ ^String catalog ^String schema-pattern ^String type-name-pattern]
   (metadata-result-set (.getSuperTypes dbmeta catalog schema-pattern type-name-pattern) opts))

  (^ResultSet getTablePrivileges [_ ^String catalog ^String schema-pattern ^String table-name-pattern]
   (metadata-result-set (.getTablePrivileges dbmeta catalog schema-pattern table-name-pattern) opts))

  (^ResultSet getTableTypes [_]
   (metadata-result-set (.getTableTypes dbmeta) opts))

  (^ResultSet getTables [_ ^String catalog ^String schema-pattern ^String table-name-pattern ^"[Ljava.lang.String;" types]
   (metadata-result-set (.getTables dbmeta catalog schema-pattern table-name-pattern types) opts))

  (^ResultSet getTypeInfo [_]
   (metadata-result-set (.getTypeInfo dbmeta) opts))

  (^ResultSet getUDTs [_ ^String catalog ^String schema-pattern ^String type-name-pattern ^ints types]
   (metadata-result-set (.getUDTs dbmeta catalog schema-pattern type-name-pattern types) opts)))

(defn proxy-database-metadata
  "Wrap a `DatabaseMetaData` in a `ProxyDatabaseMetaData`, if it is not already wrapped."
  ^ProxyDatabaseMetaData [dbmeta & [options]]
  (u/proxy-wrap ProxyDatabaseMetaData ->ProxyDatabaseMetaData dbmeta options))

(defn metadata
  "Fetch metadata about the current database."
  ^ProxyDatabaseMetaData [^Connection conn & [options]]
  (proxy-database-metadata (.getMetaData conn) (merge (options/options conn) options)))
