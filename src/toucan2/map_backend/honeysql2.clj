(ns toucan2.map-backend.honeysql2
  (:require
   [better-cond.core :as b]
   [honey.sql :as hsql]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.instance :as instance]
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
  (u/with-debug-result ["apply kv-arg %s %s" k v]
    (update honeysql :where (fn [existing-where]
                              (:where (hsql.helpers/where existing-where
                                                          (condition->honeysql-where-clause k v)))))))

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

(m/defmethod pipeline/transduce-build [#_query-type :toucan.query-type/select.*
                                       #_model      :default
                                       #_query      :toucan.map-backend/honeysql2]
  [rf query-type model {:keys [columns], :as parsed-args} resolved-query]
  (let [parsed-args    (dissoc parsed-args :columns)
        resolved-query (merge {:select (or (not-empty columns)
                                           [:*])}
                              (when model
                                {:from [(table-and-alias model)]})
                              resolved-query)]
    (next-method rf query-type model parsed-args resolved-query)))

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

(m/defmethod pipeline/transduce-build [#_query-type :toucan.query-type/insert.*
                                       #_model      :default
                                       #_query      :toucan.map-backend/honeysql2]
  [rf query-type model parsed-args resolved-query]
  (let [rows        (some (comp not-empty :rows) [parsed-args resolved-query])
        built-query (merge
                     {:insert-into [(keyword (model/table-name model))]}
                     ;; if `rows` is just a single empty row then insert it with
                     ;;
                     ;; INSERT INTO table DEFAULT VALUES
                     ;;
                     ;; syntax. See the clause registered above
                     (if (= rows [{}])
                       {::default-values true}
                       {:values (map (partial instance/instance model)
                                     rows)}))]
    ;; rows is only added so we can get the default methods' no-op logic if there are no rows at all.
    (next-method rf query-type model (assoc parsed-args :rows rows) built-query)))

(m/defmethod pipeline/transduce-build [#_query-type :toucan.query-type/update.*
                                       #_model      :default
                                       #_query      :toucan.map-backend/honeysql2]
  [rf query-type model {:keys [kv-args changes], :as parsed-args} query]
  (let [parsed-args (assoc parsed-args :kv-args (merge kv-args query))
        built-query       {:update (table-and-alias model)
                           :set    changes}]
    ;; `:changes` are added to `parsed-args` so we can get the no-op behavior in the default method.
    (next-method rf query-type model (assoc parsed-args :changes changes) built-query)))

(m/defmethod pipeline/transduce-build [#_query-type :toucan.query-type/delete.*
                                       #_model      :default
                                       #_query      :toucan.map-backend/honeysql2]
  [rf query-type model parsed-args resolved-query]
  (let [resolved-query (merge {:delete-from (table-and-alias model)}
                              resolved-query)]
    (next-method rf query-type model parsed-args resolved-query)))

;;;; Query compilation

(defonce global-options
  (atom nil))

(def ^:dynamic *options* nil)

;;; TODO -- should there be a model-specific HoneySQL options method as well? Or can model just bind
;;; [[*options*]] inside of [[toucan2.model/with-model]] if they need to do something special? I'm leaning
;;; towards the latter.

(m/defmethod pipeline/transduce-compile [#_query-type :default
                                             #_model      :default
                                             #_query      :toucan.map-backend/honeysql2]
  [rf query-type model honeysql]
  (let [options  (merge @global-options
                        *options*)
        _        (u/println-debug ["Compiling Honey SQL 2 with options %s" options])
        sql-args (u/try-with-error-context ["compile Honey SQL query" {::honeysql honeysql, ::options options}]
                                           (hsql/format honeysql options))]
    (next-method rf query-type model sql-args)))
