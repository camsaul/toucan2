(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(s/def ::default-args
  (s/cat
   :modelable ::query/default-args.modelable
   :rows      (s/alt :single-row-map    map?
                     :multiple-row-maps (s/coll-of map?)
                     :kv-pairs          ::query/default-args.kv-args
                     :columns-rows      (s/cat :columns (s/coll-of keyword?)
                                               :rows    (s/coll-of vector?)))))

(m/defmethod query/args-spec :toucan.query-type/insert.*
  [_query-type]
  ::default-args)

(m/defmethod query/parse-args :toucan.query-type/insert.*
  [query-type unparsed-args]
  (-> (next-method query-type unparsed-args)
      (select-keys [:modelable :columns :rows])
      (update :rows (fn [[rows-type x]]
                      (condp = rows-type
                        :single-row-map    [x]
                        :multiple-row-maps x
                        :kv-pairs          [(into {} (map (juxt :k :v)) x)]
                        :columns-rows      (let [{:keys [columns rows]} x]
                                             (mapv (partial zipmap columns)
                                                   rows)))))))

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

(m/defmethod query/build [:toucan.query-type/insert.* :default :default]
  [query-type model {:keys [rows], :as parsed-args}]
  (when (empty? rows)
    (throw (ex-info "Cannot build insert query with empty :values"
                    {:query-type query-type, :model model, :args parsed-args})))
  (merge
   {:insert-into [(keyword (model/table-name model))]}
   ;; if `rows` is just a single empty row then insert it with
   ;;
   ;; INSERT INTO table DEFAULT VALUES
   ;;
   ;; syntax. See the clause registered above
   (if (= rows [{}])
     {::default-values true}
     {:values (map (partial instance/instance model)
                   rows)})))

;;;; [[reducible-insert]] and [[insert!]]

(m/defmethod pipeline/transduce-resolved-query* [:toucan.query-type/insert.* :default]
  [rf query-type model {:keys [rows], :as parsed-args} resolved-query]
  (if (empty? rows)
    (do
      (u/println-debug "Query has no changes, skipping update")
      ;; TODO -- not sure this is the right thing to do
      (rf (rf)))
    (u/with-debug-result ["Inserting %s rows into %s" (count rows) model]
      (next-method rf query-type model parsed-args resolved-query))))

(defn reducible-insert
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/insert.update-count unparsed-args))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed :toucan.query-type/insert.update-count unparsed-args))

(defn reducible-insert-returning-pks
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/insert.pks unparsed-args))

(defn insert-returning-pks!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed :toucan.query-type/insert.pks unparsed-args))

(defn reducible-insert-returning-instances
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & columns-to-return] row-or-rows]
               [[modelable & columns-to-return] k v & more]
               [[modelable & columns-to-return] columns row-vectors])}
  [& unparsed-args]
  (pipeline/reducible-unparsed :toucan.query-type/insert.instances unparsed-args))

(defn insert-returning-instances!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & columns-to-return] row-or-rows]
               [[modelable & columns-to-return] k v & more]
               [[modelable & columns-to-return] columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed :toucan.query-type/insert.instances unparsed-args))
