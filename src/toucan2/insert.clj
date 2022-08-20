(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
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

;;;; [[reducible-insert]] and [[insert!]]

(m/defmulti reducible-insert*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod reducible-insert* :around :default
  [model parsed-args]
  (u/with-debug-result [(list `reducible-insert* model parsed-args)]
    (next-method model parsed-args)))

(defn reduce-reducible-insert! [model {:keys [rows], :as parsed-args} rf init]
  (if (empty? rows)
    (do
      (u/println-debug "No rows to insert.")
      (reduce rf init []))
    (u/with-debug-result ["Inserting %s rows into %s" (count rows) model]
      (let [query (query/build ::insert model (update parsed-args :rows (fn [rows]
                                                                          (mapv (partial instance/instance model)
                                                                                rows))))]
        (try
          (reduce rf init (execute/reducible-query (model/deferred-current-connectable model) model query))
          (catch Throwable e
            (throw (ex-info (format "Error updating rows: %s" (ex-message e))
                            {:model model, :args parsed-args, :query query}
                            e))))))))

(defrecord ReducibleInsert [model parsed-args]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce-reducible-insert! model parsed-args rf init))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleInsert model parsed-args)))

(m/defmethod reducible-insert* :default
  [model parsed-args]
  (->ReducibleInsert model parsed-args))

(defn reducible-insert
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (let [parsed-args (query/parse-args ::insert model unparsed-args)]
      (reducible-insert* model parsed-args))))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors])}
  [modelable & unparsed-args]
  (u/with-debug-result [(list* `insert! modelable unparsed-args)]
    (model/with-model [model modelable]
      (try
        (reduce (fnil + 0 0) 0 (apply reducible-insert model unparsed-args))
        (catch Throwable e
          (throw (ex-info (format "Error inserting %s rows: %s" (pr-str model) (ex-message e))
                          {:model model, :unparsed-args unparsed-args}
                          e)))))))

;;;; [[reducible-insert-returning-pks]] and [[insert-returning-pks!]]

(m/defmulti reducible-insert-returning-pks*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(m/defmethod reducible-insert-returning-pks* :default
  [model parsed-args]
  (select/return-pks-eduction model (reducible-insert* model parsed-args)))

(defn reducible-insert-returning-pks
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable & unparsed-args]
  (model/with-model [model modelable]
    (query/with-parsed-args-with-query [parsed-args [::insert model unparsed-args]]
      (reducible-insert-returning-pks* model parsed-args))))

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
  (u/with-debug-result [(list* `insert-returning-pks! modelable unparsed-args)]
    (realize/realize (apply reducible-insert-returning-pks modelable unparsed-args))))

;;;; [[reducible-insert-returning-instances]] and [[insert-returning-instances!]]

(m/defmulti reducible-insert-returning-instances*
  {:arglists '([model parsed-args])}
  u/dispatch-on-first-arg)

(defrecord ReducibleInsertReturningInstances [model fields reducible-returning-pks]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (when-let [row-pks (not-empty (realize/realize reducible-returning-pks))]
      (u/with-debug-result ["return instances for PKs %s" row-pks]
        (reduce
         rf
         init
         (select/select-reducible-with-pks (into [model] fields) row-pks)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->ReducibleInsertReturningInstances model fields reducible-returning-pks)))

;;; TODO -- should use `:columns`, not `:fields`

(m/defmethod reducible-insert-returning-instances* :default
  [model {:keys [fields], :as parsed-args}]
  (->ReducibleInsertReturningInstances model fields (reducible-insert-returning-pks* model parsed-args)))

(defn reducible-insert-returning-instances
  {:arglists '([modelable pk? conditions-map-or-query? & conditions-kv-args changes-map]
               [[modelable & columns] pk? conditions-map-or-query? & conditions-kv-args changes-map])}
  [modelable-columns & unparsed-args]
  (let [[modelable & columns] (if (sequential? modelable-columns)
                                modelable-columns
                                [modelable-columns])]
    (model/with-model [model modelable]
      (query/with-parsed-args-with-query [parsed-args [::insert model unparsed-args]]
        (reducible-insert-returning-instances* model (assoc parsed-args :fields columns))))))

(defn insert-returning-instances!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows]
               [modelable k v & more]
               [modelable columns row-vectors]
               [[modelable & columns] row-or-rows]
               [[modelable & columns] k v & more]
               [[modelable & columns] columns row-vectors])}
  [modelable-columns & unparsed-args]
  (u/with-debug-result [(list* `insert-returning-instances! modelable-columns unparsed-args)]
    (realize/realize (apply reducible-insert-returning-instances modelable-columns unparsed-args))))
