(ns toucan2.jdbc.sqlite
  "SQLite integration (workarounds for timestamps, generated keys, and UPDATE returning PKs)."
  (:require
   [clojure.string :as str]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.jdbc.connection]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.pipeline]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline])
  (:import
   (java.sql ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

(doseq [^String connection-class-name ["org.sqlite.SQLiteConnection"
                                        "org.sqlite.jdbc3.JDBC3Connection"
                                        "org.sqlite.jdbc4.JDBC4Connection"]]
  (when-let [connection-class (try
                                (Class/forName connection-class-name)
                                (catch Throwable _))]
    (derive connection-class ::connection)))

;;;; SQL rewriting — strip table aliases from UPDATE/DELETE
;;;
;;; SQLite doesn't support `UPDATE "table" AS "alias"` or `DELETE FROM "table" AS "alias"`.
;;; We rewrite the compiled SQL to remove aliases and alias-qualified column references.

(defn- strip-table-alias
  "Remove table alias from UPDATE/DELETE SQL that SQLite doesn't support.
  Handles both `UPDATE \"venues\" \"venue\" SET ...` (no AS) and
  `UPDATE \"venues\" AS \"venue\" SET ...` (with AS) forms.
  Also strips alias-qualified column references like `\"venue\".\"name\"`."
  [^String sql]
  (if-let [[_ stmt-type table alias] (re-find #"(?i)(UPDATE|DELETE FROM)\s+\"([^\"]+)\"\s+(?:AS\s+)?\"([^\"]+)\"" sql)]
    (let [quoted-alias (java.util.regex.Pattern/quote alias)]
      (-> sql
          ;; First strip alias-qualified column references: "alias"."col" → "col"
          (str/replace (str "\"" alias "\".") "")
          ;; Then remove the alias declaration after the table name (with or without AS)
          (str/replace (re-pattern (str "(?i)(\"" (java.util.regex.Pattern/quote table) "\")\\s+(?:AS\\s+)?\"" quoted-alias "\""))
                       "$1")))
    sql))

(defn- maybe-rewrite-sql
  "Rewrite compiled SQL query to work around SQLite limitations."
  [compiled-query]
  (if (and (sequential? compiled-query) (string? (first compiled-query)))
    (let [sql (first compiled-query)]
      (if (or (str/starts-with? (str/upper-case sql) "UPDATE")
              (str/starts-with? (str/upper-case sql) "DELETE"))
        (into [(strip-table-alias sql)] (rest compiled-query))
        compiled-query))
    compiled-query))

;;;; Foreign keys pragma — enable once per connection, not per query

(m/defmethod conn/do-with-connection ::connection
  "Enable foreign keys when opening a SQLite connection."
  [^java.sql.Connection conn f]
  (with-open [stmt (.createStatement conn)]
    (.execute stmt "PRAGMA foreign_keys = ON"))
  (f conn))

;;;; SQL rewriting — apply on each query execution

(m/defmethod pipeline/transduce-execute-with-connection :around [#_conn       ::connection
                                                                  #_query-type :default
                                                                  #_model      :default]
  "Rewrite SQL for SQLite compatibility (strip table aliases from UPDATE/DELETE)."
  [rf conn query-type model compiled-query]
  (next-method rf conn query-type model (maybe-rewrite-sql compiled-query)))

;;;; Boolean handling
;;;
;;; SQLite stores booleans as integers (0/1). The JDBC driver reports the type as Types/BOOLEAN
;;; but returns Integer values. Convert them to proper booleans.

(m/defmethod jdbc.read/read-column-thunk [#_conn  ::connection
                                           #_model :default
                                           #_type  Types/BOOLEAN]
  "SQLite stores booleans as integers (0/1). Convert them to proper booleans."
  [_conn _model ^ResultSet rset ^ResultSetMetaData _rsmeta ^Long i]
  (fn sqlite-read-boolean-thunk []
    (let [v (.getObject rset i)]
      (when (some? v)
        (not (zero? (long v)))))))

;;;; INSERT returning PKs — generated key column renaming
;;;
;;; SQLite's JDBC driver returns the generated key as `last_insert_rowid()` instead of the
;;; actual PK column name. Same pattern as MySQL/MariaDB's `insert_id`.

(m/defmethod pipeline/transduce-execute-with-connection [#_conn       ::connection
                                                          #_query-type :toucan.query-type/insert.pks
                                                          #_model      :default]
  "SQLite RETURN_GENERATED_KEYS workaround.
  - If all rows specify PKs, return them directly (skip getGeneratedKeys).
  - For single-PK auto-generated inserts, execute as update-count then compute all IDs
    from last_insert_rowid() since SQLite's getGeneratedKeys only returns the last row."
  [rf conn query-type model compiled-query]
  (let [rows                 (:rows pipeline/*parsed-args*)
        pks                  (model/primary-keys model)
        return-pks-directly? (and (seq rows)
                                  (every? (fn [row]
                                            (every? (fn [k]
                                                      (contains? row k))
                                                    pks))
                                          rows))]
    (if return-pks-directly?
      (do
        (pipeline/transduce-execute-with-connection (pipeline/default-rf :toucan.query-type/insert.update-count)
                                                    conn
                                                    :toucan.query-type/insert.update-count
                                                    model
                                                    compiled-query)
        (transduce
         (map (model/select-pks-fn model))
         rf
         rows))
      ;; For single auto-generated PK: execute as update-count, then compute all IDs from
      ;; last_insert_rowid(). SQLite guarantees contiguous rowids for a single INSERT statement.
      ;; This works for both direct rows and named queries (where *parsed-args* has no :rows).
      (if (= (count pks) 1)
        (let [update-count (pipeline/transduce-execute-with-connection
                            (pipeline/default-rf :toucan.query-type/insert.update-count)
                            conn
                            :toucan.query-type/insert.update-count
                            model
                            compiled-query)
              last-id      (with-open [stmt (.createStatement ^java.sql.Connection conn)
                                       rs   (.executeQuery stmt "SELECT last_insert_rowid()")]
                             (when (.next rs) (.getLong ^java.sql.ResultSet rs 1)))
              first-id     (- last-id (dec (long update-count)))]
          (transduce identity rf (mapv #(+ first-id %) (range update-count))))
        (next-method rf conn query-type model compiled-query)))))

(m/prefer-method! #'pipeline/transduce-execute-with-connection
                  [::connection :toucan.query-type/insert.pks :default]
                  [java.sql.Connection :toucan.result-type/pks :default])

;;;; Builder function — rename `last_insert_rowid()` to actual PK column name

(m/defmethod jdbc.rs/builder-fn [::connection :default]
  "Rename `last_insert_rowid()` to the actual PK column name for SQLite."
  [conn model rset opts]
  (let [opts               (jdbc.options/merge-options opts)
        label-fn           (get opts :label-fn name)
        model-pks          (model/primary-keys model)
        rowid-label-fn     (if (= (count model-pks) 1)
                             (fn [label]
                               (if (= label "last_insert_rowid()")
                                 (let [pk (first model-pks)
                                       pk (if (namespace pk)
                                            pk
                                            (name pk))]
                                   (log/debugf "SQLite inserted ID workaround: fetching last_insert_rowid() as %s" pk)
                                   pk)
                                 label))
                             identity)
        label-fn'          (comp label-fn rowid-label-fn)]
    (next-method conn model rset (assoc opts :label-fn label-fn'))))

;;;; UPDATE returning PKs workaround
;;;
;;; SQLite doesn't support RETURNING for UPDATE via JDBC's getGeneratedKeys().
;;; Same workaround as MySQL/MariaDB: SELECT matching PKs first, then UPDATE.

(m/defmethod pipeline/transduce-execute-with-connection [#_connection ::connection
                                                          #_query-type :toucan.query-type/update.pks
                                                          #_model      :default]
  "SQLite doesn't support returning PKs for UPDATE. SELECT matching PKs first, then UPDATE."
  [original-rf conn _query-type model sql-args]
  (let [conditions-map pipeline/*resolved-query*
        _              (log/debugf "SQLite update-returning-pks workaround: doing SELECT with conditions %s"
                                   conditions-map)
        parsed-args    (update pipeline/*parsed-args* :kv-args merge conditions-map)
        select-rf      (pipeline/conj-with-init! [])
        xform          (map (model/select-pks-fn model))
        pks            (pipeline/transduce-query (xform select-rf)
                                                  :toucan.query-type/select.instances.fns
                                                  model
                                                  parsed-args
                                                  {})]
    (log/debugf "SQLite update-returning-pks workaround: got PKs %s" pks)
    (let [update-rf (pipeline/default-rf :toucan.query-type/update.update-count)]
      (log/debugf "SQLite update-returning-pks workaround: performing original UPDATE")
      (pipeline/transduce-execute-with-connection update-rf conn :toucan.query-type/update.update-count model sql-args))
    (log/debugf "SQLite update-returning-pks workaround: transducing PKs with original reducing function")
    (transduce
     identity
     original-rf
     pks)))

(m/prefer-method! #'pipeline/transduce-execute-with-connection
                  [::connection :toucan.query-type/update.pks :default]
                  [java.sql.Connection :toucan.result-type/pks :default])
