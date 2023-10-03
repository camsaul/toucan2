(ns toucan2.jdbc
  (:require
   [methodical.core :as m]
   [toucan2.jdbc.query :as jdbc.query]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.types :as types]))

(set! *warn-on-reflection* true)

(m/defmethod pipeline/transduce-execute-with-connection [#_connection java.sql.Connection
                                                         #_query-type :default
                                                         #_model      :default]
  "Default impl for the JDBC query execution backend."
  [rf conn query-type model sql-args]
  {:pre [(sequential? sql-args) (string? (first sql-args))]}
  ;; `:return-keys` is passed in this way instead of binding a dynamic var because we don't want any additional queries
  ;; happening inside of the `rf` to return keys or whatever.
  (let [extra-options (when (isa? query-type :toucan.result-type/pks)
                        {:return-keys true})
        result        (jdbc.query/reduce-jdbc-query rf (rf) conn model sql-args extra-options)]
    (rf result)))

(m/defmethod pipeline/transduce-execute-with-connection [#_connection java.sql.Connection
                                                         #_query-type :toucan.result-type/pks
                                                         #_model      :default]
  "JDBC query execution backend for executing queries that return PKs (`:toucan.result-type/pks`).

  Applies transducer to call [[toucan2.model/select-pks-fn]] on each result row."
  [rf conn query-type model sql-args]
  (let [xform (map (model/select-pks-fn model))]
    (next-method (xform rf) conn query-type model sql-args)))

(defn- transduce-instances-from-pks
  [rf model columns pks]
  ;; make sure [[toucan2.select]] is loaded so we get the impls for `:toucan.query-type/select.instances`
  (when-not (contains? (loaded-libs) 'toucan2.select)
    (locking clojure.lang.RT/REQUIRE_LOCK
      (require 'toucan2.select)))
  (if (empty? pks)
    []
    (let [kv-args     {:toucan/pk [:in pks]}
          parsed-args {:columns columns
                       :kv-args kv-args}]
      (pipeline/transduce-query rf :toucan.query-type/select.instances-from-pks model parsed-args {}))))

(derive ::DML-queries-returning-instances :toucan.result-type/instances)

(doseq [query-type [:toucan.query-type/delete.instances
                    :toucan.query-type/update.instances
                    :toucan.query-type/insert.instances]]
  (derive query-type ::DML-queries-returning-instances))

(m/defmethod pipeline/transduce-execute-with-connection [#_connection java.sql.Connection
                                                         #_query-type ::DML-queries-returning-instances
                                                         #_model      :default]
  "DML queries like `UPDATE` or `INSERT` don't usually support returning instances, at least not with JDBC. So for these
  situations we'll fake it by first running an equivalent query returning inserted/affected PKs, and then do a
  subsequent SELECT to get those rows. Then we'll reduce the rows with the original reducing function."
  [rf conn query-type model sql-args]
  ;; We're using `conj` here instead of `rf` so no row-transform nonsense or whatever is done. We will pass the
  ;; actual instances to the original `rf` once we get them.
  (let [pk-query-type (types/similar-query-type-returning query-type :toucan.result-type/pks)
        pks           (pipeline/transduce-execute-with-connection conj conn pk-query-type model sql-args)
        ;; this is sort of a hack but I don't know of any other way to pass along `:columns` information with the
        ;; original parsed args
        columns       (:columns pipeline/*parsed-args*)]
    ;; once we have a sequence of PKs then get instances as with `select` and do our magic on them using the
    ;; ORIGINAL `rf`.
    (transduce-instances-from-pks rf model columns pks)))

;;;; load the miscellaneous integrations

(defn- class-exists? [^String class-name]
  (try
    (Class/forName class-name)
    (catch Throwable _)))

(when (class-exists? "org.postgresql.jdbc.PgConnection")
  (require 'toucan2.jdbc.postgres))

(when (some class-exists? ["org.mariadb.jdbc.Connection"
                           "org.mariadb.jdbc.MariaDbConnection"
                           "com.mysql.cj.MysqlConnection"])
  (require 'toucan2.jdbc.mysql-mariadb))
