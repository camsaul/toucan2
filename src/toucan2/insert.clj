(ns toucan2.insert
  "Implementation of [[insert!]].

  The code for building an INSERT query as Honey SQL lives in [[toucan2.honeysql2]]"
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

(s/def ::kv-args
  (s/+ (s/cat
        :k keyword?
        :v any?)))

(s/def ::args.rows
  (s/alt :nil               nil?
         :single-row-map    map?
         :multiple-row-maps (s/spec (s/* map?))
         :kv-pairs          ::kv-args
         :columns-rows      (s/cat :columns (s/spec (s/+ keyword?))
                                   :rows    (s/spec (s/+ sequential?)))))

(s/def ::args
  (s/cat
   :connectable       ::query/default-args.connectable
   :modelable         ::query/default-args.modelable
   :rows-or-queryable (s/alt :rows      ::args.rows
                             :queryable some?)))

(m/defmethod query/parse-args :toucan.query-type/insert.*
  "Default args parsing method for [[toucan2.insert/insert!]]. Uses the spec `:toucan2.insert/args`."
  [query-type unparsed-args]
  (let [parsed                               (query/parse-args-with-spec query-type ::args unparsed-args)
        [rows-queryable-type rows-queryable] (:rows-or-queryable parsed)
        parsed                               (select-keys parsed [:modelable :columns :connectable])]
    (case rows-queryable-type
      :queryable
      (assoc parsed :queryable rows-queryable)

      :rows
      (assoc parsed :rows (let [[rows-type x] rows-queryable]
                            (condp = rows-type
                              :nil               nil
                              :single-row-map    [x]
                              :multiple-row-maps x
                              :kv-pairs          [(into {} (map (juxt :k :v)) x)]
                              :columns-rows      (let [{:keys [columns rows]} x]
                                                   (map (partial zipmap columns)
                                                        rows))))))))

(defn- can-skip-insert? [parsed-args resolved-query]
  (and (empty? (:rows parsed-args))
       ;; don't try to optimize out stuff like identity query.
       (or (and (map? resolved-query)
                (empty? (:rows resolved-query)))
           (nil? resolved-query))))

(m/defmethod pipeline/build [#_query-type     :toucan.query-type/insert.*
                             #_model          :default
                             #_resolved-query :default]
  "Default INSERT query method. No-ops if there are no `:rows` to insert in either `parsed-args` or `resolved-query`."
  [query-type model parsed-args resolved-query]
  (let [rows (some (comp not-empty :rows) [parsed-args resolved-query])]
    (if (can-skip-insert? parsed-args resolved-query)
      (do
        (log/debugf "Query has no changes, skipping update")
        ::pipeline/no-op)
      (do
        (log/debugf "Inserting %s rows into %s" (if (seq rows) (count rows) "?") model)
        (next-method query-type model parsed-args resolved-query)))))

(defn insert!
  "Insert a row or rows into the database. Returns the number of rows inserted.

  This function is pretty flexible in what it accepts:

  Insert a single row with key-value args:

  ```clj
  (t2/insert! :models/venues :name \"Grant & Green\", :category \"bar\")
  ```

  Insert a single row as a map:

  ```clj
  (t2/insert! :models/venues {:name \"Grant & Green\", :category \"bar\"})
  ```

  Insert multiple row maps:

  ```clj
  (t2/insert! :models/venues [{:name \"Grant & Green\", :category \"bar\"}
                              {:name \"Savoy Tivoli\", :category \"bar\"}])
  ```

  Insert rows with a vector of column names and a vector of value maps:

  ```clj
  (t2/insert! :models/venues [:name :category] [[\"Grant & Green\" \"bar\"]
                                                [\"Savoy Tivoli\" \"bar\"]])
  ```

  As with other Toucan 2 functions, you can optionally pass a connectable if you pass `:conn` as the first arg. Refer to
  the `:toucan2.insert/args` spec for the complete syntax.

  Named connectables can also be used to define the rows:

  ```clj
  (t2/define-named-query ::named-rows
    {:rows [{:name \"Grant & Green\", :category \"bar\"}
            {:name \"North Beach Cantina\", :category \"restaurant\"}]})

  (t2/insert! :models/venues ::named-rows)
  ```"
  {:arglists '([modelable row-or-rows-or-queryable]
               [modelable k v & more]
               [modelable columns row-vectors]
               [:conn connectable modelable row-or-rows]
               [:conn connectable modelable k v & more]
               [:conn connectable modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.update-count unparsed-args))

(defn insert-returning-pks!
  "Like [[insert!]], but returns a vector of the primary keys of the newly inserted rows rather than the number of rows
  inserted. The primary keys are determined by [[model/primary-keys]]. For models with a single primary key, this
  returns a vector of single values, e.g. `[1 2]` if the primary key is `:id` and you've inserted rows 1 and 2; for
  composite primary keys this returns a vector of tuples where each tuple has the value of corresponding primary key as
  returned by [[model/primary-keys]], e.g. for composite PK `[:id :name]` you might get `[[1 \"Cam\"] [2 \"Sam\"]]`."
  {:arglists '([modelable row-or-rows-or-queryable]
               [modelable k v & more]
               [modelable columns row-vectors]
               [:conn connectable modelable row-or-rows]
               [:conn connectable modelable k v & more]
               [:conn connectable modelable columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.pks unparsed-args))

(defn insert-returning-pk!
  "Like [[insert-returning-pks!]], but for one-row insertions. For models with a single primary key, this returns just the
  new primary key as a scalar value (e.g. `1`). For models with a composite primary key, it will return a single tuple
  as determined by [[model/primary-keys]] (e.g. `[1 \"Cam\"]`)."
  {:arglists '([modelable row-or-rows-or-queryable]
               [modelable k v & more]
               [modelable columns row-vectors]
               [:conn connectable modelable row-or-rows]
               [:conn connectable modelable k v & more]
               [:conn connectable modelable columns row-vectors])}
  [& unparsed-args]
  (first (apply insert-returning-pks! unparsed-args)))

(defn insert-returning-instances!
  "Like [[insert!]], but returns a vector of maps representing the inserted objects."
  {:arglists '([modelable-columns row-or-rows-or-queryable]
               [modelable-columns k v & more]
               [modelable-columns columns row-vectors]
               [:conn connectable modelable-columns row-or-rows]
               [:conn connectable modelable-columns k v & more]
               [:conn connectable modelable-columns columns row-vectors])}
  [& unparsed-args]
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.instances unparsed-args))

(defn insert-returning-instance!
  "Like [[insert-returning-instances!]], but for one-row insertions. Returns the inserted object as a map."
  {:arglists '([modelable row-or-rows-or-queryable]
               [modelable k v & more]
               [modelable columns row-vectors]
               [:conn connectable modelable row-or-rows]
               [:conn connectable modelable k v & more]
               [:conn connectable modelable columns row-vectors])}
  [& unparsed-args]
  (first (apply insert-returning-instances! unparsed-args)))
