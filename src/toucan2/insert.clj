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
   [toucan2.realize :as realize]
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
   :values      rows})

(m/defmulti insert!*
  "Returns the number of rows inserted."
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod insert!* :around :default
  [model parsed-args]
  (u/with-debug-result [(list `insert!* model parsed-args)]
    (next-method model parsed-args)))

(def ^:dynamic *result-type*
  "Type of results we want when inserting something. Either `:reducible` for reducible results, or `:row-count` for the
  number of rows inserted. `:reducible` executes the query with [[execute/reducible-query]] while `:row-count`
  uses [[query/execute!]]."
  :row-count)

(defn- execute! [model query]
  (let [connectable (model/deferred-current-connectable model)
        execute-fn  (case *result-type*
                      :reducible execute/reducible-query
                      :row-count execute/query-one)]
    (execute-fn connectable query)))

(m/defmethod insert!* :default
  [model {:keys [rows], :as parsed-args}]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      (case *result-type*
        :row-count 0
        :reducible []))
    ;; TODO -- should this stuff be in an `:around` method?
    (u/with-debug-result ["Inserting %s rows into %s" (count rows) model]
      (let [query (query/build ::insert model (update parsed-args :rows (fn [rows]
                                                                          (mapv (partial instance/instance model)
                                                                                rows))))]
        (try
          (execute! model query)
          (catch Throwable e
            (throw (ex-info (format "Error inserting rows: %s" (ex-message e))
                            {:model model, :args parsed-args, :query query}
                            e))))))))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & args]
  (u/with-debug-result [(list* `insert! modelable args)]
    (model/with-model [model modelable]
      (insert!* model (query/parse-args ::insert model args)))))

(m/defmulti insert-returning-keys!*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod insert-returning-keys!* :default
  [model parsed-args]
  (binding [*result-type*           :reducible
            t2.jdbc.query/*options* (assoc t2.jdbc.query/*options* :return-keys true)]
    (into
     []
     (map (select/select-pks-fn model))
     (insert!* model parsed-args))))

(m/defmethod insert-returning-keys!* :around :default
  [model parsed-args]
  (u/with-debug-result [(list `insert-returning-keys!* model parsed-args)]
    (next-method model parsed-args)))

;;; TODO -- rename to `insert-returning-pks!`
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
  (u/with-debug-result [(list* `insert-returning-keys! modelable args)]
    (model/with-model [model modelable]
      (try
        (insert-returning-keys!* model (query/parse-args ::insert model args))
        (catch Throwable e
          (throw (ex-info (format "Error in %s for %s: %s" `insert-returning-keys! (pr-str model) (ex-message e))
                          {:model model, :args args}
                          e)))))))

(m/defmulti insert-returning-instances!*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod insert-returning-instances!* :around :default
  [model parsed-args]
  (u/with-debug-result [(list `insert-returning-instances!* model parsed-args)]
    (next-method model parsed-args)))

;;; TODO -- should this actually be `reducible-insert-returning-instances!*`? Seems like it should.

(m/defmethod insert-returning-instances!* :default
  [model {:keys [fields], :as  parsed-args}]
  (when-let [row-pks (not-empty (insert-returning-keys!* model parsed-args))]
    (u/println-debug ["%s returned %s" `insert-returning-keys!* row-pks])
    (realize/realize (select/select-reducible-with-pks (into [model] fields) row-pks))))

(defn insert-returning-instances!
  {:arglists '([modelable & row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & fields] & row-or-rows]
               [[modelable & fields] k v & more]
               [[modelable & fields] columns row-vectors])}
  [modelable-fields & args]
  (u/with-debug-result [(list* `insert-returning-instances! modelable-fields args)]
    (try
      (let [[modelable & fields] (if (sequential? modelable-fields)
                                   modelable-fields
                                   [modelable-fields])]
        (model/with-model [model modelable]
          (let [parsed-args (query/parse-args ::insert model args)]
            (insert-returning-instances!* model (assoc parsed-args :fields fields)))))
      (catch Throwable e
        (throw (ex-info (format "Error in %s: %s" `insert-returning-instances! (ex-message e))
                        {:modelable modelable-fields, :args args}
                        e))))))
