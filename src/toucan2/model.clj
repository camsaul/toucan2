(ns toucan2.model
  (:require
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.current :as current]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

;; TODO -- this should probably also support.
(m/defmulti do-with-model
  {:arglists '([modelable f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-model :default
  [model f]
  (f model))

(defmacro with-model [[model-binding modelable] & body]
  `(do-with-model ~modelable (^:once fn* [model#]
                              (binding [current/*model* model#]
                                (let [~model-binding model#]
                                  ~@body)))))

(defrecord ReducibleQueryAs [connectable modelable query]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (with-model [_model modelable]
      (reduce rf init (query/reducible-query connectable query))
      #_(binding [query/*jdbc-options* (merge
                                      {:builder-fn (instance/instance-result-set-builder model)}
                                      query/*jdbc-options*)]
        (reduce rf init (query/reducible-query connectable query)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query-as connectable modelable query)))

(defn reducible-query-as [connectable modelable query]
  (->ReducibleQueryAs connectable modelable query))

(defn query-as [connectable modelable query]
  (realize/realize (reducible-query-as connectable modelable query)))

(m/defmulti default-connectable
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmulti table-name
  {:arglists '([model])}
  u/dispatch-on-first-arg)

(m/defmethod table-name :default
  [model]
  (name model))

(m/defmulti primary-keys
  {:arglists '([model])}
  u/dispatch-on-first-arg)

;; if the PK comes back unwrapped, wrap it.
(m/defmethod primary-keys :around :default
  [model]
  (let [pk (next-method model)]
    (if (sequential? pk)
      pk
      [pk])))

(m/defmethod primary-keys :default
  [_model]
  [:id])

;;;; [[build-select-query]]

(m/defmulti build-select-query
  {:arglists '([model query columns conditions])}
  u/dispatch-on-first-two-args)

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
                            (pr-str (u/dispatch-on-first-two-args model query)))
                    {:model model, :query query, :columns columns, :conditions conditions})))
  query)

(m/defmulti apply-condition
  {:arglists '([model query k v])}
  u/dispatch-on-first-three-args)

(defn condition->honeysql-where-clause [k v]
  (if (sequential? v)
    (vec (list* (first v) k (rest v)))
    [:= k v]))

(m/defmethod apply-condition [:default clojure.lang.IPersistentMap :default]
  [_model honeysql k v]
  (update honeysql :where (fn [existing-where]
                            (:where (hsql.helpers/where existing-where
                                                        (condition->honeysql-where-clause k v))))))

(m/defmethod apply-condition [:default clojure.lang.IPersistentMap :toucan2/pk]
  [model honeysql _k v]
  (let [pk-columns (primary-keys model)
        v          (if (sequential? v)
                     v
                     [v])]
    (assert (= (count pk-columns)
               (count v))
            (format "Expected %s primary key values for %s, got %d values %s"
                    (count pk-columns) (pr-str pk-columns)
                    (count v) (pr-str v)))
    (reduce
     (fn [honeysql [k v]]
       (apply-condition model honeysql k v))
     honeysql
     (zipmap pk-columns v))))

(m/defmethod build-select-query [:default clojure.lang.IPersistentMap]
  [model query columns conditions]
  (let [honeysql (merge {:select (or (not-empty columns)
                                     [:*])}
                        (when model
                          {:from [[(keyword (table-name model))]]})
                        query)]
    (reduce
     (fn [honeysql [k v]]
       (apply-condition model honeysql k v))
     honeysql
     conditions)))

(m/defmethod build-select-query [:default Long]
  [model id columns conditions]
  (let [pks (primary-keys model)]
    (assert (= (count pks) 1)
            (format "Cannot build query for model %s from integer %d: expected one primary key, got %s"
                    (pr-str model) id (pr-str pks)))
    (let [pk (first pks)]
      (build-select-query model {} columns (assoc conditions pk id)))))
