(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.select :as select]))

(s/def ::default-args
  (s/alt :single-row-map    map?
         :multiple-row-maps (s/coll-of map?)
         :kv-pairs          (s/* (s/cat
                                  :k keyword?
                                  :v any?))
         :columns-rows      (s/cat :columns (s/coll-of keyword?)
                                   :rows    (s/coll-of vector?))))

(m/defmulti parse-args
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod parse-args :after :default
  [_model parsed]
  (u/println-debug (format "Parsed args: %s" (pr-str parsed)))
  parsed)

(m/defmethod parse-args :default
  [_model args]
  (let [parsed (s/conform ::default-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret insert! args: %s" (s/explain-str ::default-args args))
                      {:args args})))
    (let [[rows-type x] parsed]
      (condp = rows-type
        :single-row-map    [x]
        :multiple-row-maps x
        :kv-pairs          [(into {} (map (juxt :k :v)) x)]
        :columns-rows      (let [{:keys [columns rows]} x]
                             (for [row rows]
                               (zipmap columns row)))))))

(m/defmulti insert!*
  "Returns the number of rows inserted."
  {:arglists '([model args])}
  u/dispatch-on-first-arg)

(m/defmethod insert!* :around :default
  [model args]
  (u/with-debug-result (pr-str (list 'insert!* model args))
    (next-method model args)))

(defn build-query
  "Default way of building queries for the default impl of [[insert!]]."
  [model rows]
  (let [query {:insert-into [(keyword (model/table-name model))]
               :values      rows}]
    (u/println-debug (format "Build insert query: %s" (pr-str query)))
    query))

(def ^:dynamic *result-type*
  "Type of results we want when inserting something. Either `:reducible` for reducible results, or `:row-count` for the
  number of rows inserted. `:reducible` executes the query with [[query/reducible-query]] while `:row-count`
  uses [[query/execute!]]."
  :row-count)

(defn- execute! [model query]
  (let [connectable (model/default-connectable model)]
    (case *result-type*
      :reducible
      (query/reducible-query connectable query)

      :row-count
      (let [result (query/execute! connectable query)]
        (or (-> result first :next.jdbc/update-count)
            result)))))

(m/defmethod insert!* :default
  [model rows]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      0)
    (u/with-debug-result (format "Inserting %d rows into %s" (count rows) (pr-str model))
      (let [query (build-query model rows)]
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
      (insert!* model (parse-args model args)))))

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

;; TODO -- I don't 100% remember why this returns the PK values as opposed to the entire row or whatever the database
;; decides to gives us. Seems not that useful TBH
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
  (u/with-debug-result (pr-str (list* 'insert-returning-keys! modelable args))
    (model/with-model [model modelable]
      (apply insert-returning-keys!* model args))))
