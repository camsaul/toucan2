(ns toucan2.model
  (:require
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]))

;; TODO -- this should probably also support.
(m/defmulti do-with-model
  {:arglists '([modelable f])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod do-with-model :default
  [model f]
  (f model))

(defmacro with-model [[model-binding modelable] & body]
  `(do-with-model ~modelable (^:once fn* [~model-binding] ~@body)))

(defrecord ReducibleModelQuery [connectable modelable compileable]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce
     rf
     init
     (with-model [model modelable]
       (eduction
        (map (fn [row]
               (list 'magic-map model (into {} row))))
        (query/reducible-query connectable compileable)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-model-query connectable modelable compileable)))

(defn reducible-model-query [connectable modelable compileable]
  (->ReducibleModelQuery connectable modelable compileable))

(m/defmulti default-connectable
  {:arglists '([model])}
  u/dispatch-on-keyword-or-type-1)

(m/defmulti table-name
  {:arglists '([model])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod table-name :default
  [model]
  (name model))

(m/defmulti primary-keys
  {:arglists '([model])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod primary-keys :default
  [_model]
  [:id])


;;;; [[build-select-query]]

(m/defmulti build-select-query
  {:arglists '([model query columns conditions])}
  u/dispatch-on-keyword-or-type-2)

(m/defmethod build-select-query :around :default
  [model query columns conditions]
  (u/with-debug-result (pr-str (list `build-select-query model query columns conditions))
    (next-method model query columns conditions)))

(m/defmethod build-select-query :default
  [model query columns conditions]
  (when (or (seq columns)
            (seq conditions))
    (throw (ex-info (format (str "Don't know how to build select query for %s from query ^%s %s with columns or "
                                 "conditions. Do you need to implement build-select-query for %s?")
                            (pr-str model)
                            (some-> query class .getCanonicalName)
                            (pr-str query)
                            (pr-str (u/dispatch-on-keyword-or-type-2 model query)))
                    {:model model, :query query, :columns columns, :conditions conditions})))
  query)

(defn condition->honeysql-where-clause [[k v]]
  (if (sequential? v)
    (vec (list* (first v) k (rest v)))
    [:= k v]))

(defn conditions->honeysql-where-clause [conditions]
  (when (seq conditions)
    (let [clauses (map condition->honeysql-where-clause conditions)]
      (if (= (count clauses) 1)
        (first clauses)
        (into [:and] clauses)))))

(m/defmethod build-select-query [:default clojure.lang.IPersistentMap]
  [model query columns conditions]
  (cond-> (merge {:select (or (not-empty columns)
                              [:*])}
                 (when model
                   {:from [[(keyword (table-name model))]]})
                 query)
    (seq conditions) (update :where (fn [existing-where]
                                      (:where (hsql.helpers/where existing-where
                                                                  (conditions->honeysql-where-clause conditions)))))))

(m/defmethod build-select-query [:default Long]
  [model id columns conditions]
  (let [pks (primary-keys model)]
    (assert (= (count pks) 1)
            (format "Cannot build query for model %s from integer %d: expected one primary key, got %s"
                    (pr-str model) id (pr-str pks)))
    (let [pk (first pks)]
      (build-select-query model {} columns (assoc conditions pk id)))))
