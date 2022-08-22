(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.operation :as op]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(s/def ::default-args
  (s/alt :single-row-map    map?
         :multiple-row-maps (s/coll-of map?)
         :kv-pairs          (s/* (s/cat
                                  :k keyword?
                                  :v any?))
         :columns-rows      (s/cat :columns (s/coll-of keyword?)
                                   :rows    (s/coll-of vector?))))

(m/defmethod query/args-spec [::insert :default]
  [_query-type _model]
  ::default-args)

(m/defmethod query/parse-args [::insert :default]
  [query-type model unparsed-args]
  (let [[rows-type x] (next-method query-type model unparsed-args)]
    {:rows (condp = rows-type
             :single-row-map    [x]
             :multiple-row-maps x
             :kv-pairs          [(into {} (map (juxt :k :v)) x)]
             :columns-rows      (let [{:keys [columns rows]} x]
                                  (mapv (partial zipmap columns)
                                        rows)))}))

(m/defmethod query/build [::insert :default :default]
  [query-type model {:keys [rows], :as parsed-args}]
  (when (empty? rows)
    (throw (ex-info "Cannot build insert query with empty :values"
                    {:query-type query-type, :model model, :args parsed-args})))
  {:insert-into [(keyword (model/table-name model))]
   :values      (map (partial instance/instance model)
                     rows)})

;;;; [[reducible-insert]] and [[insert!]]

(m/defmethod op/reducible* [::insert :default]
  [query-type model {:keys [rows], :as parsed-args}]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      nil)
    (u/with-debug-result ["Inserting %s rows into %s" (count rows) model]
      (next-method query-type model parsed-args))))

(defn reducible-insert
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (op/reducible ::insert modelable unparsed-args))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (op/returning-update-count! ::insert modelable unparsed-args))

(defn reducible-insert-returning-pks
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (op/reducible-returning-pks ::insert modelable unparsed-args))

(defn insert-returning-pks!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (op/returning-pks! ::insert modelable unparsed-args))


(defn reducible-insert-returning-instances
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & columns-to-return] row-or-rows]
               [[modelable & columns-to-return] k v & more]
               [[modelable & columns-to-return] columns row-vectors])}
  [modelable-columns & unparsed-args]
  (op/reducible-returning-instances ::insert modelable-columns unparsed-args))

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
  [modelable-columns & unparsed-args]
  (op/returning-instances! ::insert modelable-columns unparsed-args))
