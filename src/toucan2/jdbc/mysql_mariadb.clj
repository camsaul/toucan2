(ns toucan2.jdbc.mysql-mariadb
  "MySQL and MariaDB integration (mostly workarounds for broken stuff)."
  (:require
   [methodical.core :as m]
   [next.jdbc]
   [next.jdbc.prepare :as next.jdbc.prepare]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.read :as jdbc.read]
   [toucan2.jdbc.result-set :as jdbc.rs]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]
   [toucan2.util :as u])
  (:import
   (java.sql PreparedStatement ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

;;; TODO -- need the MySQL class here too.

(doseq [^String connection-class-name ["org.mariadb.jdbc.Connection"
                                       "org.mariadb.jdbc.MariaDbConnection"
                                       "com.mysql.cj.MysqlConnection"]]
  (when-let [connection-class (try
                                (Class/forName connection-class-name)
                                (catch Throwable _))]
    (derive connection-class ::connection)))

(m/defmethod jdbc.read/read-column-thunk [#_conn  ::connection
                                          #_model :default
                                          #_type  Types/TIMESTAMP]
  "MySQL/MariaDB `timestamp` is normalized to UTC, so return it as an `OffsetDateTime` rather than a `LocalDateTime`.
  `datetime` columns should be returned as `LocalDateTime`. Both `timestamp` and `datetime` seem to come back as
  `java.sql.Types/TIMESTAMP`, so check the actual database column type name so we can fetch objects as the correct
  class."
  [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
  (let [^Class klass (if (= (u/lower-case-en (.getColumnTypeName rsmeta i)) "timestamp")
                       java.time.OffsetDateTime
                       java.time.LocalDateTime)]
    (jdbc.read/get-object-of-class-thunk rset i klass)))

(m/prefer-method! #'jdbc.read/read-column-thunk
                  [::connection :default Types/TIMESTAMP]
                  [java.sql.Connection :default Types/TIMESTAMP])

;;;; INSERT RETURN_GENERATED_KEYS workarounds

(defn- single-row-sql-args
  "Compile a single-row version of the current INSERT for each row in `rows`, using the same build/compile pipeline the
  original multi-row query went through."
  [query-type model rows]
  (mapv (fn [row]
          (pipeline/compile query-type model
                            (pipeline/build query-type model
                                            (assoc pipeline/*parsed-args* :rows [row])
                                            pipeline/*resolved-query*)))
        rows))

(defn- normalize-generated-key
  "Bring integral generated keys back to `Long`.

  Nothing server-side is new here: the MySQL wire protocol has always reported `last_insert_id` as an unsigned
  64-bit integer. What changed in mariadb-java-client 3.x is how the driver types the synthetic `getGeneratedKeys`
  result set it builds from that value: 2.x declared the column a (signed) BIGINT and returned `Long`; 3.x declares
  it `BIGINT UNSIGNED` — faithful to the protocol, since the value could in principle exceed `Long/MAX_VALUE` — and
  the driver's type mapping for unsigned BIGINT is `java.math.BigInteger`. Downstream code dispatches on PK class —
  e.g. `(select model pk)` builds a PK query for `Long` but passes a `BigInteger` through as if it were a compiled
  query — so coerce keys back to `Long` (`.longValueExact` throws rather than truncates in the fanciful case of a
  key beyond `Long/MAX_VALUE`)."
  [v]
  (cond
    (instance? java.math.BigInteger v) (.longValueExact ^java.math.BigInteger v)
    (vector? v)                        (mapv normalize-generated-key v)
    :else                              v))

(defn- execute-batched-inserts-returning-key-rows!
  "Execute compiled single-row INSERT `sql-argses` as JDBC batches — one batch per consecutive run of identical SQL —
  and return the generated-key rows in insertion order."
  [conn model sql-argses]
  (let [opts (jdbc.options/merge-options nil)]
    (reduce (fn [acc sql-args-group]
              (let [sql (ffirst sql-args-group)]
                (with-open [ps (next.jdbc/prepare conn [sql] (assoc opts :return-keys true))]
                  (doseq [sql-args sql-args-group]
                    (next.jdbc.prepare/set-parameters ps (vec (rest sql-args)))
                    (.addBatch ^PreparedStatement ps))
                  (.executeBatch ^PreparedStatement ps)
                  (with-open [rset (.getGeneratedKeys ^PreparedStatement ps)]
                    ;; key rows are lazily backed by the live ResultSet -- realize them before it closes
                    (jdbc.rs/reduce-result-set ((map realize/realize) conj) acc conn model rset opts)))))
            []
            (partition-by first sql-argses))))

(m/defmethod pipeline/transduce-execute-with-connection [#_conn       ::connection
                                                         #_query-type :toucan.query-type/insert.pks
                                                         #_model      :default]
  "Two workarounds:

  1. Apparently `RETURN_GENERATED_KEYS` doesn't work for MySQL/MariaDB if values for the primary key are specified in
  the INSERT itself *and* the primary key is not an integer. So look at the rows we're inserting: if every row
  specifies the primary key column(s) (including `nil` values), transduce those specified values rather than what JDBC
  returns.

  This seems like it won't work if these values were arbitrary Honey SQL expressions. I suppose we could work around
  THAT problem by running the primary key values thru another SELECT query... but that just seems like too much. I
  guess we can cross that bridge when we get there.

  2. The server only reports the FIRST generated key for a multi-row `INSERT ... VALUES (...), (...)`. The 2.x MariaDB
  connector fabricated the remaining keys arithmetically from `auto_increment_increment` — plausible on a single-node
  server, silently wrong on Galera/multi-master (where the increment changes with cluster topology) and for mixed
  explicit-PK inserts — and the 3.x connector stopped guessing and returns only the first key. Executing the same
  insert as a JDBC *batch* of single-row statements sidesteps all of that: the driver reports each statement's own
  server-reported key. So multi-row inserts are recompiled per row and executed as a batch."
  [rf conn query-type model compiled-query]
  ;; rows can come from the parsed args or from the resolved query (e.g. named queries) -- same lookup the
  ;; Honey SQL build method does
  (let [rows                 (some (comp not-empty :rows) [pipeline/*parsed-args* pipeline/*resolved-query*])
        pks                  (model/primary-keys model)
        return-pks-directly? (and (seq rows)
                                  (every? (fn [row]
                                            (every? (fn [k]
                                                      (contains? row k))
                                                    pks))
                                          rows))]
    (cond
      return-pks-directly?
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

      ;; multi-row insert relying on generated keys: recompile per row and execute as a batch (see workaround 2 above)
      (next rows)
      (let [key-rows (execute-batched-inserts-returning-key-rows!
                      conn
                      model
                      (single-row-sql-args query-type model rows))]
        (log/debugf "batched multi-row insert workaround: got %s generated key rows" (count key-rows))
        (transduce
         (comp (map (model/select-pks-fn model))
               (map normalize-generated-key))
         rf
         key-rows))

      :else
      (next-method ((map normalize-generated-key) rf) conn query-type model compiled-query))))

(m/prefer-method! #'pipeline/transduce-execute-with-connection
                  [::connection :toucan.query-type/insert.pks :default]
                  [java.sql.Connection :toucan.result-type/pks :default])

;;;; UPDATE returning PKs workaround

;;; MySQL and MariaDB don't support returning PKs for UPDATE, so we'll have to hack it as follows:
;;;
;;; 1. Rework the original query to be a SELECT, run it, and record the matching PKs somewhere. Currently only supported
;;;    for queries we can manipulate e.g. Honey SQL
;;;
;;; 2. Run the original UPDATE query
;;;
;;; 3. Return the PKs from the rewritten SELECT query

(m/defmethod pipeline/transduce-execute-with-connection [#_connection ::connection
                                                         #_query-type :toucan.query-type/update.pks
                                                         #_model      :default]
  "MySQL and MariaDB don't support returning PKs for UPDATE. Execute a SELECT query to capture the PKs of the rows that
  will be affected BEFORE performing the UPDATE. We need to capture PKs for both `:toucan.query-type/update.pks` and for
  `:toucan.query-type/update.instances`, since ultimately the latter is implemented on top of the former."
  [original-rf conn _query-type model sql-args]
  ;; if for some reason we've already captured PKs, don't do it again.
  (let [conditions-map pipeline/*resolved-query*
        _              (log/debugf "update-returning-pks workaround: doing SELECT with conditions %s"
                                   conditions-map)
        parsed-args    (update pipeline/*parsed-args* :kv-args merge conditions-map)
        select-rf      (pipeline/conj-with-init! [])
        xform          (map (model/select-pks-fn model))
        pks            (pipeline/transduce-query (xform select-rf)
                                                 :toucan.query-type/select.instances.fns
                                                 model
                                                 parsed-args
                                                 {})]
    (log/debugf "update-returning-pks workaround: got PKs %s" pks)
    (let [update-rf (pipeline/default-rf :toucan.query-type/update.update-count)]
      (log/debugf "update-returning-pks workaround: performing original UPDATE")
      (pipeline/transduce-execute-with-connection update-rf conn :toucan.query-type/update.update-count model sql-args))
    (log/debugf "update-returning-pks workaround: transducing PKs with original reducing function")
    (transduce
     identity
     original-rf
     pks)))

(m/prefer-method! #'pipeline/transduce-execute-with-connection
                  [::connection :toucan.query-type/update.pks :default]
                  [java.sql.Connection :toucan.result-type/pks :default])

;;;; Builder function

(m/defmethod jdbc.rs/builder-fn [::connection :default]
  "This is an icky hack for MariaDB/MySQL. Inserted rows come back with the newly inserted ID as `:insert-id` rather than
  the actual name of the primary key column. So tweak the `:label-fn` we pass to `next.jdbc` to rename `:insert-id` to
  the actual PK name we'd expect. This only works for tables with a single-column PK."
  [conn model rset opts]
  (let [opts               (jdbc.options/merge-options opts)
        label-fn           (get opts :label-fn name)
        model-pks          (model/primary-keys model)
        insert-id-label-fn (if (= (count model-pks) 1)
                             (fn [label]
                               (if (= label "insert_id")
                                 (let [pk (first model-pks)
                                       ;; there is some weirdness afoot. If we return a keyword without a namespace
                                       ;; then `next.jdbc` seems to qualify it regardless of whether the
                                       ;; `:qualifier-fn` returns `nil` or not -- so a PK like `:id` gets returned
                                       ;; as `(keyword "" "id")`. But that doesn't happen if the label function
                                       ;; returns a String.
                                       ;;
                                       ;; It seems like returning a string is the preferred thing to do, but in some
                                       ;; cases [[model/primary-keys]] returns a namespaced keyword, and we want to
                                       ;; preserve that namespace; `next.jdbc` does not try to change keywords that
                                       ;; already have namespaces.
                                       ;;
                                       ;; So return the PK name as a keyword if the PK keyword is namespaced;
                                       ;; otherwise return a string.
                                       pk (if (namespace pk)
                                            pk
                                            (name pk))]
                                   (log/debugf "MySQL/MariaDB inserted ID workaround: fetching insert_id as %s" pk)
                                   pk)
                                 label))
                             identity)
        label-fn'          (comp label-fn insert-id-label-fn)]
    (next-method conn model rset (assoc opts :label-fn label-fn'))))
