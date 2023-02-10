(ns toucan2.map-backend.honeysql2
  (:require
   [better-cond.core :as b]
   [clojure.string :as str]
   [honey.sql :as hsql]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(derive :toucan.map-backend/honeysql2 :toucan.map-backend/*)

(defonce ^{:doc "Default global options to pass to [[honey.sql/format]]."} global-options
  (atom {:quoted true, :dialect :ansi, :quoted-snake true}))

(def ^:dynamic *options*
  "Option override when to pass to [[honey.sql/format]]."
  nil)

(defn options
  "Get combined Honey SQL options for building and compiling queries by merging [[global-options]] and [[*options*]]."
  []
  (merge @global-options
         *options*))

;;;; Building queries

(defn- fn-condition->honeysql-where-clause
  [k [f & args]]
  {:pre [(keyword? f) (seq args)]}
  (into [f k] args))

(defn condition->honeysql-where-clause
  "Something sequential like `:id [:> 5]` becomes `[:> :id 5]`. Other stuff like `:id 5` just becomes `[:= :id 5]`."
  [k v]
  ;; don't think there's any situation where `nil` on the LHS is on purpose and not a bug.
  {:pre [(some? k)]}
  (if (sequential? v)
    (fn-condition->honeysql-where-clause k v)
    [:= k v]))

(m/defmethod query/apply-kv-arg [#_model :default #_query :toucan.map-backend/honeysql2 #_k :default]
  "Apply key-value args to a Honey SQL 2 query map."
  [_model honeysql k v]
  (log/debugf  :compile "apply kv-arg %s %s" k v)
  (let [result (update honeysql :where (fn [existing-where]
                                         (:where (hsql.helpers/where existing-where
                                                                     (condition->honeysql-where-clause k v)))))]
    (log/tracef :compile "=> %s" result)
    result))

(defn table-and-alias
  "Build an Honey SQL `[table]` or `[table alias]` (if the model has a [[toucan2.model/namespace]] form) for `model` for
  use in something like a `:select` clause."
  [model]
  (b/cond
    :let [table-id (keyword (model/table-name model))
          alias-id (model/namespace model)
          alias-id (when alias-id
                     (keyword alias-id))]
    alias-id
    [table-id alias-id]

    :else
    [table-id]))

;;; Qualify the (plain keyword) columns in [model & columns] forms with the model table name, unless they are already
;;; qualified. This apparently doesn't hurt anything and prevents ambiguous column errors if you're joining another
;;; column or something like that. I wasn't going to put this in at first since I forgot it existed, but apparently
;;; Toucan 1 did it (despite not being adequately tested) so I decided to preserve this behavior going forward since I
;;; can't see any downsides to it.
;;;
;;; In Honey SQL 2 I think using keyword namespaces is the preferred way to qualify stuff, so we'll go that route
;;; instead of using `:a.b` style qualification like we generated in Toucan 1.

(defn- qualified? [column]
  (or (namespace column)
      (str/includes? (name column) ".")))

(defn- maybe-qualify [column table]
  (cond
    (not (keyword? column)) column
    (qualified? column)     column
    :else                   (keyword (name table) (name column))))

(defn- maybe-qualify-columns [columns [table-id alias-id]]
  (let [table (or alias-id table-id)]
    (assert (keyword? table))
    (mapv #(maybe-qualify % table)
          columns)))

(defn include-default-select?
  "Should we splice in the default `:select` clause for this `honeysql-query`? Only splice in the default `:select` if we
  don't have `:union`, `:union-all`, or `:select-distinct` in the resolved query."
  [honeysql-query]
  (every? (fn [k]
            (not (contains? honeysql-query k)))
          [:union :union-all :select :select-distinct]))

(defn- include-default-from?
  "Should we splice in the default `:from` clause for this `honeysql-query`? Only splice in the default `:from` if we
  don't have `:union` or `:union-all` in the resolved query. It doesn't make sense to do a `x UNION y` query and then
  include `FROM` as well."
  [honeysql-query]
  (every? (fn [k]
            (not (contains? honeysql-query k)))
          [:union :union-all]))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build a Honey SQL 2 SELECT query."
  [query-type model {:keys [columns], :as parsed-args} resolved-query]
  (log/debugf :compile "Building SELECT query for %s with columns %s" model columns)
  (let [parsed-args    (dissoc parsed-args :columns)
        table+alias    (table-and-alias model)
        resolved-query (-> (merge
                            (when (include-default-select? resolved-query)
                              {:select (or (some-> (not-empty columns) (maybe-qualify-columns table+alias))
                                           [:*])})
                            (when (and model
                                       (include-default-from? resolved-query))
                              {:from [table+alias]})
                            resolved-query)
                           (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" resolved-query)
    (next-method query-type model parsed-args resolved-query)))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.count
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build an efficient `count(*)` query to power [[toucan2.select/count]]."
  [query-type model parsed-args resolved-query]
  (let [parsed-args (assoc parsed-args :columns [[:%count.* :count]])]
    (next-method query-type model parsed-args resolved-query)))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.exists
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build an efficient query like `SELECT exists(SELECT 1 FROM ...)` query to power [[toucan2.select/exists?]]."
  [query-type model parsed-args resolved-query]
  (let [parsed-args (assoc parsed-args :columns [[[:inline 1]]])
        subselect   (next-method query-type model parsed-args resolved-query)]
    {:select [[[:exists subselect] :exists]]}))

(defn- empty-insert [_model dialect]
  (if (#{:mysql :mariadb} dialect)
    {:columns []
     :values  [[]]}
    {:values :default}))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/insert.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build a Honey SQL 2 INSERT query.

  if `rows` is just a single empty row then insert it with

  ```sql
  INSERT INTO table DEFAULT VALUES
  ```

  (Postgres/H2/etc.)

  or

  ```sql
  INSERT INTO table () VALUES ()
  ```

  (MySQL/MariaDB)"
  [query-type model parsed-args resolved-query]
  (log/debugf :compile "Building INSERT query for %s" model)
  (let [rows        (some (comp not-empty :rows) [parsed-args resolved-query])
        built-query (-> (merge {:insert-into [(keyword (model/table-name model))]}
                               (if (= rows [{}])
                                 (empty-insert model (:dialect (options)))
                                 {:values (map (partial instance/instance model)
                                               rows)}))
                        (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" built-query)
    ;; rows is only added so we can get the default methods' no-op logic if there are no rows at all.
    (next-method query-type model (assoc parsed-args :rows rows) built-query)))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/update.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build a Honey SQL 2 UPDATE query."
  [query-type model {:keys [kv-args changes], :as parsed-args} conditions-map]
  (log/debugf :compile "Building UPDATE query for %s" model)
  (let [parsed-args (assoc parsed-args :kv-args (merge kv-args conditions-map))
        built-query (-> {:update (table-and-alias model)
                         :set    changes}
                        (with-meta (meta conditions-map)))]
    (log/debugf :compile "=> %s" built-query)
    ;; `:changes` are added to `parsed-args` so we can get the no-op behavior in the default method.
    (next-method query-type model (assoc parsed-args :changes changes) built-query)))

;;; For building a SELECT query using the args passed to something like [[toucan2.update/update!]]. This is needed to
;;; implement [[toucan2.tools.before-update]]. The main syntax difference is a map 'resolved-query' is supposed to be
;;; treated as a conditions map for update instead of as a raw Honey SQL query.
;;;
;;; TODO -- a conditions map should probably not be given a type of `:toucan.map-backend/honeysql2` -- conditions maps
;;; should be a separate map backend I think.

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.instances.from-update
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Treat the resolved query as a conditions map but otherwise behave the same as the `:toucan.query-type/select.instances`
  impl."
  [query-type model parsed-args conditions-map]
  (next-method query-type model (update parsed-args :kv-args #(merge % conditions-map)) {}))

(defn- delete-from
  "Build the correct `DELETE ... FROM ...` or `DELETE FROM ...` Honey SQL for the current `dialect` (see docstring for
  build method below)."
  [table+alias dialect]
  (if (= (count table+alias) 1)
    {:delete-from table+alias}
    (let [[_table table-alias] table+alias]
      (if (#{:mysql :mariadb} dialect)
        {:delete table-alias
         :from   [table+alias]}
        {:delete-from table+alias}))))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/delete.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Build a Honey SQL 2 DELETE query.

  If the table for `model` should not be aliased (i.e., [[toucan2.model/namespace]] returns `nil`), builds a query that
  compiles to something like:

  ```sql
  DELETE FROM my_table
  WHERE ...
  ```

  If the table is aliased, this looks like

  ```sql
  DELETE FROM my_table AS t1
  WHERE ...
  ```

  for Postgres/H2/etc., or like

  ```sql
  DELETE t1
  FROM my_table AS t1
  WHERE ...
  ```

  for MySQL/MariaDB. MySQL/MariaDB does not seem to support aliases in `DELETE FROM`, so we need to use this alternative
  syntax; H2 doesn't support it however. So it has to be compiled differently based on the DB."
  [query-type model parsed-args resolved-query]
  (log/debugf :compile "Building DELETE query for %s" model)
  (let [built-query (-> (merge (delete-from (table-and-alias model) (:dialect (options)))
                               resolved-query)
                        (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" built-query)
    (next-method query-type model parsed-args built-query)))

;;;; Query compilation

(m/defmethod pipeline/compile [#_query-type :default
                               #_model      :default
                               #_query      :toucan.map-backend/honeysql2]
  "Compile a Honey SQL 2 map to [sql & args]."
  [query-type model honeysql]
  (let [options-map (options)
        _           (log/debugf :compile "Compiling Honey SQL 2 with options %s" options-map)
        sql-args    (u/try-with-error-context ["compile Honey SQL to SQL" {::honeysql honeysql, ::options-map options-map}]
                      (hsql/format honeysql options-map))]
    (log/debugf :compile "=> %s" sql-args)
    (pipeline/compile query-type model sql-args)))
