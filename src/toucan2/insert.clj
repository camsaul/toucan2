(ns toucan2.insert
  "Implementation of [[insert!]]."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]))

(s/def ::args.rows
  (s/alt :nil               nil?
         :single-row-map    map?
         :multiple-row-maps (s/spec (s/* map?))
         :kv-pairs          ::query/default-args.kv-args.non-empty
         :columns-rows      (s/cat :columns (s/spec (s/+ keyword?))
                                   :rows    (s/spec (s/+ sequential?)))))

(s/def ::args
  (s/cat
   :modelable         ::query/default-args.modelable
   :rows-or-queryable (s/alt :rows      ::args.rows
                             :queryable some?)))

(defn parse-insert-args
  [query-type unparsed-args]
  (let [parsed                               (query/parse-args query-type ::args unparsed-args)
        [rows-queryable-type rows-queryable] (:rows-or-queryable parsed)
        parsed                               (select-keys parsed [:modelable :columns])]
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
                                                   (mapv (partial zipmap columns)
                                                         rows))))))))

(m/defmethod pipeline/transduce-unparsed :toucan.query-type/insert.*
  [rf query-type unparsed-args]
  (let [parsed-args (parse-insert-args query-type unparsed-args)]
    (pipeline/transduce-parsed rf query-type parsed-args)))

(defn- can-skip-insert? [parsed-args resolved-query]
  (and (empty? (:rows parsed-args))
       ;; don't try to optimize out stuff like identity query.
       (or (and (map? resolved-query)
                (empty? (:rows resolved-query)))
           (nil? resolved-query))))

(m/defmethod pipeline/transduce-build [#_query-type :toucan.query-type/insert.*
                                                #_model      :default
                                                #_query      :default]
  [rf query-type model parsed-args resolved-query]
  (let [rows (some (comp not-empty :rows) [parsed-args resolved-query])]
    (if (can-skip-insert? parsed-args resolved-query)
      (do
        (log/debugf :compile "Query has no changes, skipping update")
        ;; TODO -- not sure this is the right thing to do
        (rf (rf)))
      (do
        (log/debugf :compile "Inserting %s rows into %s" (if (seq rows) (count rows) "?") model)
        (next-method rf query-type model parsed-args resolved-query)))))

;;; The code for building an INSERT query as Honey SQL lives in [[toucan2.map-backend.honeysql2]]

;;;; [[reducible-insert]] and [[insert!]]

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
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.update-count unparsed-args))

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
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.pks unparsed-args))

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
  (pipeline/transduce-unparsed-with-default-rf :toucan.query-type/insert.instances unparsed-args))
