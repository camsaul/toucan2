(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.model :as model]
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

(m/defmethod insert!* :default
  [model rows]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      0)
    (u/with-debug-result (format "Inserting %d rows into %s" (count rows) (pr-str model))
      (let [query (build-query model rows)]
        (try
          (let [result (query/execute! (model/default-connectable model) query)]
            (or (-> result first :next.jdbc/update-count)
                result))
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

#_(defn insert-returning-keys!
  {:arglists '([connectable-tableable row-or-rows options?]
               [connectable-tableable k v & more options?]
               [connectable-tableable columns row-vectors options?])}
  [connectable-tableable & args]
  (let [{:keys [modelable query options]} (parse-insert-args connectable-tableable args)
        options                                       (-> options
                                                          ;; TODO -- this is next.jdbc specific
                                                          (assoc-in [:next.jdbc :return-keys] true)
                                                          (assoc :reducible? true))
        reducible-query (do-insert! modelable query options)]
    (into
     []
     (map (select/select-pks-fn modelable))
     reducible-query)))
