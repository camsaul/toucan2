(ns toucan.db
  "Helper functions for querying the DB and inserting or updating records using Toucan models."
  (:refer-clojure :exclude [count])
  (:require [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [honeysql.helpers :as h]
            [methodical.core :as m]
            [toucan.common :as common]
            [toucan.db :as db]
            [toucan2.compile :as compile]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            toucan2.honeysql
            toucan2.jdbc
            [toucan2.log :as log]
            [toucan2.mutative :as mutative]
            [toucan2.query :as query]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]))

(comment toucan2.jdbc/keep-me
         toucan2.honeysql/keep-me)

(derive :toucan1/connectable :toucan2/jdbc)
(derive :toucan1/connectable :toucan2/honeysql)

(defonce ^:private toucan1-options
  (atom {:honeysql {:quoting             :ansi
                    :allow-dashed-names? true}}))

(def ^:dynamic *quoting-style* nil)

(defn set-default-quoting-style! [new-quoting-style]
  (swap! toucan1-options assoc-in [:honeysql :quoting] new-quoting-style))

(defn quoting-style []
  (let [options (conn.current/default-options-for-connectable* (conn.current/current-connectable))]
    (get-in options [:honeysql :quoting])))

(def ^:dynamic *automatically-convert-dashes-and-underscores* nil)

(defn set-default-automatically-convert-dashes-and-underscores!
  "Set the default value for allowing dashes in field names. Defaults to `true`."
  [new-value]
  (swap! toucan1-options assoc-in [:honeysql :allow-dashed-names?] (not new-value)))

(defn automatically-convert-dashes-and-underscores? []
  (let [options (conn.current/default-options-for-connectable* (conn.current/current-connectable))]
    (not (get-in options [:honeysql :allow-dashed-names?]))))

(m/defmethod conn.current/default-options-for-connectable* :toucan1/connectable
  [_]
  (cond-> @toucan1-options
    *quoting-style*
    (assoc-in [:honeysql :quoting] *quoting-style*)

    (some? *automatically-convert-dashes-and-underscores*)
    (assoc-in [:honeysql :allow-dashed-names?] (not *automatically-convert-dashes-and-underscores*))))

;; instead of `*db-connection*`, you can look at `conn.current/*current-connectable*`
#_(def ^:dynamic *db-connection* nil)

(defn set-default-db-connection!
  "Set the JDBC connecton details map for the default application DB connection. This connection is used by default by
  the various `toucan.db` functions.

   `db-connection-map` is passed directly to `clojure.java.jdbc`; it can be anything that is accepted by it.

     (db/set-default-db-connection!
       {:classname   \"org.postgresql.Driver\"
        :subprotocol \"postgresql\"
        :subname     \"//localhost:5432/my_db\"
        :user        \"cam\"})"
  {:style/indent 0}
  [jdbc-spec]
  (derive :toucan2/default :toucan1/connectable))

(defn set-default-jdbc-options!
  [jdbc-options]
  (swap! toucan1-options assoc :next.jdbc jdbc-options))

;; instead of `*transaction-connection*`, look at `conn.current/*current-connectable*`
#_(def ^:dynamic *transaction-connection*
  "Transaction connection to the application DB. Used internally by `transaction`."
    nil)

(defn connection
  "Returns a new Connection. IMPORTANT -- MAKE SURE YOU CLOSE IT WHEN DONE!!!"
  ^java.sql.Connection []
  (:connection (conn/connection)))

(defmacro transaction
  {:arglists '([& body]), :style/indent 0}
  [& body]
  `(conn/with-transaction [~'_]
     ~@body))

(defn quote-fn
  "The function that JDBC should use to quote identifiers for our database. This is passed as the `:entities` option
  to functions like `jdbc/insert!`."
  []
  ((quoting-style) @(resolve 'honeysql.format/quote-fns))) ; have to call resolve because it's not public

(defmacro with-call-counting
  {:style/indent 1}
  [[call-count-fn-binding] & body]
  `(query/with-call-count [~call-count-fn-binding]
     ~@body))

(defmacro debug-count-calls
  "Print the number of DB calls executed inside `body` to `stdout`. Intended for use during REPL development."
  {:style/indent 0}
  [& body]
  `(with-call-counting [call-count#]
     (let [results# (do ~@body)]
       (println "DB Calls:" (call-count#))
       results#)))

(defmacro debug-print-queries
  {:style/indent 0}
  [& body]
  `(log/with-debug-logging
     ~@body))

(defn honeysql->sql
  [honeysql-form]
  (compile/compile (vary-meta honeysql-form assoc :type :toucan2/honeysql)))

(defn query
  [honeysql-form & {:as options}]
  (query/query honeysql-form #_(vary-meta honeysql-form assoc :type :toucan2/honeysql)))

(defn reducible-query
  [honeysql-form & {:as options}]
  (assert (map? honeysql-form))
  (query/reducible-query (vary-meta honeysql-form assoc :type :toucan2/honeysql)))

(defn qualify [tableable field-name]
  (hsql/qualify (tableable/table-name tableable) field-name))

(defn qualified?
  [field-name]
  (if (vector? field-name)
    (qualified? (first field-name))
    (boolean (re-find #"\." (name field-name)))))

#_(defn do-post-select
  {:style/indent 1}
  [model rows]
  (let [model            (common/resolve-model model)
        key-transform-fn (if-not (automatically-convert-dashes-and-underscores?)
                           identity
                           (partial transform-keys replace-underscores))]
    (select/select model (identity-query/identity-query rows))))

(defn simple-select
  {:style/indent 1}
  [model honeysql-form]
  (let [model (common/resolve-model model)]
    (select/select
     (tableable/table-name model)
     (vary-meta honeysql-form assoc :type :toucan2.honeysql/select-query))))

(defn simple-select-reducible
  {:style/indent 1}
  [model honeysql-form]
  (let [model (common/resolve-model model)]
    (select/select-reducible
     (tableable/table-name model)
     (vary-meta honeysql-form assoc :type :toucan2.honeysql/select-query))))

(defn simple-select-one
  ([model]
   (simple-select-one model {}))
  ([model honeysql-form]
   (query/reduce-first (simple-select-reducible model honeysql-form))))

(defn execute!
  [honeysql-form & {:as options}]
  (query/execute! nil nil (vary-meta honeysql-form assoc :toucan2/honeysql) {:next.jdbc options}))

(defn update!
  "Update a single row in the database. Returns `true` if a row was affected, `false` otherwise."
  (^Boolean [model honeysql-form]
   (let [model (common/resolve-model model)]
     (pos? (query/execute! (-> (h/update model)
                               (merge honeysql-form)
                               (vary-meta assoc :type :toucan2.honeysql/update-query))))))

  (^Boolean [model id kvs]
   {:pre [(some? id) (map? kvs) (every? keyword? (keys kvs))]}
   (pos? (mutative/update! model id kvs)))

  (^Boolean [model id k v & more]
   (update! model id (apply array-map k v more))))

(defn update-where!
  "Convenience for updating several objects matching `conditions-map`. Returns `true` if any objects were affected. Does
  not call `:before` methods."
  {:style/indent 2}
  ^Boolean [model conditions-map & {:as values}]
  {:pre [(map? conditions-map) (every? keyword? (keys values))]}
  (let [model (common/resolve-model model)
        table (tableable/table-name model)]
    (mutative/update! table conditions-map values)))

(defn update-non-nil-keys!
  "Like `update!`, but filters out KVS with `nil` values."
  {:style/indent 2}
  ([model id kvs]
   (update! model id (into {} (for [[k v] kvs
                                     :when (not (nil? v))]
                                 [k v]))))
  ([model id k v & more]
   (update-non-nil-keys! model id (apply array-map k v more))))

#_(defn get-inserted-id
  "Get the ID of a row inserted by `jdbc/db-do-prepared-return-keys`."
  [primary-key insert-result]
  (when insert-result
    (some insert-result (cons primary-key inserted-id-keys))))

(defn simple-insert-many!
  "Do a simple JDBC `insert!` of multiple objects into the database.
  Normally you should use `insert-many!` instead, which calls the model's `pre-insert` method on the `row-maps`;
  `simple-insert-many!` is offered for cases where you'd like to specifically avoid this behavior. Returns a sequences
  of IDs of newly inserted objects."
  {:style/indent 1}
  [model row-maps]
  {:pre [(sequential? row-maps) (every? map? row-maps)]}
  (when (seq row-maps)
    (let [model (common/resolve-model model)
          table (tableable/table-name model)]
      (mutative/insert-returning-keys! table row-maps))))

(defn insert-many!
  "Insert several new rows into the Database. Resolves `entity`, and calls `pre-insert` on each of the `row-maps`.
  Returns a sequence of the IDs of the newly created objects.

  Note: this *does not* call `post-insert` on newly created objects. If you need `post-insert` behavior, use
  `insert!` instead."
  {:style/indent 1}
  [model row-maps]
  (let [model (common/resolve-model model)]
    ;; TODO -- this is *NOT* supposed to call `post-insert`.
    (mutative/insert-returning-keys! model row-maps)
    #_(simple-insert-many! model (for [row-map row-maps]
                                 (models/do-pre-insert model row-map)))))

(defn simple-insert!
  "Do a simple JDBC `insert` of a single object.
  This is similar to `insert!` but returns the ID of the newly created object rather than the object itself,
  and does not call `pre-insert` or `post-insert`.

     (db/simple-insert! 'Label :name \"Toucan Friendly\") -> 1

  Like `insert!`, `simple-insert!` can be called with either a single `row-map` or kv-style arguments."
  {:style/indent 1}
  ([model row-map]
   {:pre [(map? row-map) (every? keyword? (keys row-map))]}
   (first (simple-insert-many! model [row-map])))

  ([model k v & more]
   (simple-insert! model (apply array-map k v more))))

(defn insert!
  "Insert a new object into the Database. Resolves `entity`, calls its `pre-insert` method on `row-map` to prepare
  it before insertion; after insert, it fetches and the newly created object, passes it to `post-insert`, and
  returns the results.

  For flexibility, `insert!` can handle either a single map or individual kwargs:

     (db/insert! Label {:name \"Toucan Unfriendly\"})
     (db/insert! 'Label :name \"Toucan Friendly\")"
  {:style/indent 1}
  ([model row-map]
   {:pre [(map? row-map) (every? keyword? (keys row-map))]}
   (let [model (common/resolve-model model)
         pks   (mutative/insert-returning-keys! model row-map)
         rows  (when (seq pks)
                 (select/select-one model :toucan2/with-pks pks))]
     rows))

  ([model k v & more]
   (insert! model (apply array-map k v more))))

(defn- select-args [[table-and-fields & args :as all-args]]
  (log/with-trace ["Parse legacy Toucan 1 select args %s" all-args]
    (let [[table & fields] (if (and (sequential? table-and-fields)
                                    (not= (first table-and-fields) 'quote))
                             table-and-fields
                             [table-and-fields])
          table            (common/resolve-model table)
          [args query]     (if (map? (last args))
                             [(butlast args) (last args)]
                             [args nil])
          query            (cond-> query
                             (seq fields) (assoc :select (vec fields)))
          args             (if query
                             (conj (vec args) query)
                             args)
          args             (cons table args)]
      args)))

(defn select-one [& args]
  (apply select/select-one (select-args args)))

(defn select-one-field [field & args]
  (apply select/select-one-fn field (select-args args)))

(defn select-one-id [& args]
  (apply select/select-one-pk (select-args args)))

(defn count [& args]
  (apply select/count (select-args args)))

(defn select [& args]
  (apply select/select (select-args args)))

(defn select-reducible [& args]
  (apply select/select-reducible (select-args args)))

(defn select-field [field & args]
  (apply select/select-fn-set field (select-args args)))

(defn select-ids [& args]
  (apply select/select-pks-set (select-args args)))

(defn select-field->field
  [f1 f2 & args]
  (apply select/select-fn->fn f1 f2 (select-args args)))

(defn select-field->id [field & args]
  (apply select/select-fn->pk field (select-args args)))

(defn select-id->field
  [field & args]
  (apply select/select-pk->fn field (select-args args)))

(defn exists? [& args]
  (apply select/exists? (select-args args)))

(defn simple-delete!
  "Returns `true` if something was deleted, `false` otherwise."
  ([model]
   (pos? (mutative/delete! (tableable/table-name (common/resolve-model model)))))

  ([model conditions]
   {:pre [(map? conditions) (every? keyword? (keys conditions))]}
   (apply simple-delete! model (mapcat identity conditions)))

  ([model k v & more]
   (pos? (apply mutative/delete! (tableable/table-name (common/resolve-model model)) k v more))))

(defn delete!
  "Returns `true` if something was deleted, `false` otherwise."
  [model & conditions]
  (let [model (common/resolve-model model)]
    (pos? (apply mutative/delete! model conditions))))

(doseq [[_ varr] (ns-interns *ns*)]
  (alter-meta! varr assoc :deprecated true))
