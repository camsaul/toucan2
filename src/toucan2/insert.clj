(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
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
    (condp = rows-type
      :single-row-map    [x]
      :multiple-row-maps x
      :kv-pairs          [(into {} (map (juxt :k :v)) x)]
      :columns-rows      (let [{:keys [columns rows]} x]
                           (for [row rows]
                             (zipmap columns row))))))

(m/defmethod query/build [::insert :default :default]
  [_query-type model rows]
  {:insert-into [(keyword (model/table-name model))]
   :values      rows})

(m/defmulti insert!*
  "Returns the number of rows inserted."
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod insert!* :around :default
  [model args]
  (u/with-debug-result (pr-str (list 'insert!* model args))
    (next-method model args)))

(def ^:dynamic *result-type*
  "Type of results we want when inserting something. Either `:reducible` for reducible results, or `:row-count` for the
  number of rows inserted. `:reducible` executes the query with [[execute/reducible-query]] while `:row-count`
  uses [[query/execute!]]."
  :row-count)

(defn- execute! [model query]
  (let [connectable (model/deferred-current-connectable model)]
    (case *result-type*
      :reducible
      (execute/reducible-query connectable query)

      :row-count
      (execute/query-one connectable query))))

(m/defmethod insert!* :default
  [model rows]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      0)
    ;; TODO -- should this stuff be in an `:around` method?
    (u/with-debug-result (format "Inserting %d rows into %s" (count rows) (pr-str model))
      (let [query (query/build ::insert model (map (partial instance/instance model)
                                                   rows))]
        (try
          (execute! model query)
          (catch Throwable e
            (throw (ex-info (format "Error inserting rows: %s" (ex-message e))
                            {:model model, :query query}
                            e))))))))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & args]
  (u/with-debug-result (pr-str (list* 'insert! modelable args))
    (model/with-model [model modelable]
      (insert!* model (query/parse-args ::insert model args)))))

(m/defmulti insert-returning-keys!*
  {:arglists '([model & args])}
  u/dispatch-on-first-arg)

(m/defmethod insert-returning-keys!* :default
  [model & args]
  (binding [*result-type*           :reducible
            t2.jdbc.query/*options* (assoc t2.jdbc.query/*options* :return-keys true)]
    (into
     []
     (map (select/select-pks-fn model))
     (apply insert! model args))))

(defn insert-returning-keys!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & args]
  (u/with-debug-result (pr-str (list* `insert-returning-keys! modelable args))
    (model/with-model [model modelable]
      (apply insert-returning-keys!* model args))))

(defn insert-returning-instances!
  {:arglists '([modelable & row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & fields] & row-or-rows]
               [[modelable & fields] k v & more]
               [[modelable & fields] columns row-vectors])}
  [modelable-fields & args]
  (u/with-debug-result (pr-str (list* `insert-returning-instances! modelable-fields args))
    (let [[modelable & fields] (if (sequential? modelable-fields)
                                 modelable-fields
                                 [modelable-fields])]
      (model/with-model [model modelable]
        (when-let [row-pks (not-empty (apply insert-returning-keys! modelable args))]
          (let [pk-vecs      (for [pk row-pks]
                               (if (sequential? pk)
                                 pk
                                 [pk]))
                pk-keys      (model/primary-keys-vec model)
                pk-maps      (for [pk-vec pk-vecs]
                               (zipmap pk-keys pk-vec))
                conditions   (mapcat
                              (juxt identity (fn [k]
                                               [:in (mapv k pk-maps)]))
                              pk-keys)
                model-fields (if (seq fields)
                               (cons model fields)
                               model)]
            (apply select/select model-fields conditions)))))))
