(ns toucan2.map-backend.honeysql2
  (:require
   [better-cond.core :as b]
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

;;;; Building queries

(defn- fn-condition->honeysql-where-clause
  [k [f & args]]
  {:pre [(keyword? f) (seq args)]}
  (into [f k] args))

(defn condition->honeysql-where-clause
  "Something sequential like `:id [:> 5]` becomes `[:> :id 5]`. Other stuff like `:id 5` just becomes `[:= :id 5]`."
  [k v]
  ;; don't think there's any situtation where `nil` on the LHS is on purpose and not a bug.
  {:pre [(some? k)]}
  (if (sequential? v)
    (fn-condition->honeysql-where-clause k v)
    [:= k v]))

(m/defmethod query/apply-kv-arg [#_model :default #_query :toucan.map-backend/honeysql2 #_k :default]
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

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  [query-type model {:keys [columns], :as parsed-args} resolved-query]
  (log/debugf :compile "Building SELECT query for %s with columns %s" model columns)
  (let [parsed-args    (dissoc parsed-args :columns)
        resolved-query (-> (merge
                            ;; only splice in the default `:select` and `:from` if we don't have `:union` or
                            ;; `:union-all` in the resolved query. It doesn't make sense to do a `x UNION y` query and
                            ;; then include `FROM` as well
                            (when-not ((some-fn :union :union-all) resolved-query)
                              (merge
                               {:select (or (not-empty columns)
                                            [:*])}
                               (when model
                                 {:from [(table-and-alias model)]})))
                            resolved-query)
                           (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" resolved-query)
    (next-method query-type model parsed-args resolved-query)))

;;; Support
;;;
;;;    INSERT INTO table DEFAULT VALUES
;;;
;;; syntax. Not currently part of Honey SQL -- see upstream issue https://github.com/seancorfield/honeysql/issues/423
(hsql/register-clause!
 ::default-values
 (fn [_clause _value]
   ["DEFAULT VALUES"])
 nil)

(m/defmethod pipeline/build [#_query-type :toucan.query-type/insert.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  [query-type model parsed-args resolved-query]
  (log/debugf :compile "Building INSERT query for %s" model)
  (let [rows        (some (comp not-empty :rows) [parsed-args resolved-query])
        built-query (-> (merge
                         {:insert-into [(keyword (model/table-name model))]}
                         ;; if `rows` is just a single empty row then insert it with
                         ;;
                         ;; INSERT INTO table DEFAULT VALUES
                         ;;
                         ;; syntax. See the clause registered above
                         (if (= rows [{}])
                           {::default-values true}
                           {:values (map (partial instance/instance model)
                                         rows)}))
                        (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" built-query)
    ;; rows is only added so we can get the default methods' no-op logic if there are no rows at all.
    (next-method query-type model (assoc parsed-args :rows rows) built-query)))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/update.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
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

(m/defmethod pipeline/build [#_query-type :toucan.query-type/select.instances.from-update
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  "Treat the resolved query as a conditions map but otherwise behave the same as the `:toucan.query-type/select.instances`
  impl."
  [query-type model parsed-args conditions-map]
  (next-method query-type model (update parsed-args :kv-args #(merge % conditions-map)) {}))

(m/defmethod pipeline/build [#_query-type :toucan.query-type/delete.*
                             #_model      :default
                             #_query      :toucan.map-backend/honeysql2]
  [query-type model parsed-args resolved-query]
  (log/debugf :compile "Building DELETE query for %s" model)
  (let [built-query (-> (merge {:delete-from (table-and-alias model)}
                               resolved-query)
                        (with-meta (meta resolved-query)))]
    (log/debugf :compile "=> %s" built-query)
    (next-method query-type model parsed-args built-query)))

;;;; Query compilation

(def global-options
  "Default global options to pass to [[honey.sql/format]]."
  (atom {:quoted true, :dialect :ansi, :quoted-snake true}))

(def ^:dynamic *options*
  "Option override when to pass to [[honey.sql/format]]."
  nil)

(m/defmethod pipeline/compile [#_query-type :default
                               #_model      :default
                               #_query      :toucan.map-backend/honeysql2]
  [query-type model honeysql]
  (let [options  (merge @global-options
                        *options*)
        _        (log/debugf :compile "Compiling Honey SQL 2 with options %s" options)
        sql-args (u/try-with-error-context ["compile Honey SQL query" {::honeysql honeysql, ::options options}]
                   (hsql/format honeysql options))]
    (log/debugf :compile "=> %s" sql-args)
    (pipeline/compile query-type model sql-args)))
