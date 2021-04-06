(ns bluejdbc.table-aware
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as conn]
            [bluejdbc.instance :as instance]
            [bluejdbc.query :as query]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

;; TODO - consider whether there's any possible reason in the whole world to have this also dispatch on connection
;;
;; TODO -- this should work with objects as well
;;
;; TODO -- is this supposed to return a String or a Keyword? Need to document.
;;
(m/defmulti table-name
  {:arglists '([connectable tableable])}
  u/dispatch-on-first-two-args)

(m/defmethod table-name :default
  [_ this]
  this)

(m/defmethod table-name [:default String]
  [_ s]
  s)

(defn- raw-identifier [connectable identifier options]
  (compile/quote-identifier connectable identifier options))

(m/defmulti primary-key*
  {:arglists '([table object])}
  u/dispatch-on-first-arg)

(m/defmethod primary-key* :default
  [_ _]
  :id)

(defn primary-key
  ([object]       (primary-key* (instance/table object) object))
  ([table object] (primary-key* table                   object)))

(m/defmulti select*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod select* [:default :default nil]
  [connectable tableable _ options]
  (select* connectable tableable {} options))

(m/defmethod select* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable honeysql-form options]
  (let [honeysql-form (merge {:select [:*]
                              :from   [(table-name connectable tableable)]}
                             honeysql-form)]
    (query/query connectable tableable honeysql-form options)))

;; TODO -- what about stuff like (db/select Table :id 100)?
(defn select
  ([tableable]                           (select* :default    tableable nil   nil))
  ([tableable query]                     (select* :default    tableable query nil))
  ([connectable tableable query]         (select* connectable tableable query nil))
  ([connectable tableable query options] (select* connectable tableable query options)))

(m/defmulti select-one*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod select-one* [:default :default nil]
  [connectable tableable _ options]
  (select-one* connectable tableable {} options))

(m/defmethod select-one* [:default :default clojure.lang.IPersistentMap]
  [connectable tableable honeysql-form options]
  (let [honeysql-form (merge {:select [:*]
                              :from   [(table-name connectable tableable)]}
                             honeysql-form)]
    (query/query-one connectable tableable honeysql-form options)))

(defn select-one
  ([tableable]                           (select-one* :current    tableable nil   nil))
  ([tableable query]                     (select-one* :current    tableable query nil))
  ([connectable tableable query]         (select-one* connectable tableable query nil))
  ([connectable tableable query options] (select-one* connectable tableable query options)))

;; TODO -- a common method for saving or inserting stuff?

(m/defmulti insert!*
  {:arglists '([connectable tableable columns rows options])}
  u/dispatch-on-first-two-args)

(m/defmethod insert!* :default
  [connectable tableable columns rows options]
  (try
    (let [honeysql-form (merge {:insert-into (raw-identifier connectable (table-name connectable tableable) options)
                                :values      rows}
                               (when (seq columns)
                                 {:columns (mapv #(raw-identifier connectable % options)
                                                 columns)}))]
      (try
        (query/execute! connectable tableable honeysql-form options)
        (catch Throwable e
          (throw (ex-info "Error inserting rows"
                          {:honeysql-form honeysql-form}
                          e)))))
    (catch Throwable e
      (throw (ex-info "Error inserting rows"
                      {:table tableable, :columns columns, :rows (take 10 rows), :options options}
                      e)))))

(defn- one-or-many [row-or-rows]
  (cond
    (sequential? row-or-rows) row-or-rows
    (nil? row-or-rows)        nil
    :else                     [row-or-rows]))

;; TODO - can we make this argslist [connectable? tableable columns? row-or-rows options?]

(defn insert!
  ([tableable row-or-rows]                             (insert!* :current    tableable nil     (one-or-many row-or-rows) nil))
  ([tableable columns row-or-rows]                     (insert!* :current    tableable columns (one-or-many row-or-rows) nil))
  ([connectable tableable columns row-or-rows]         (insert!* connectable tableable columns (one-or-many row-or-rows) nil))
  ([connectable tableable columns row-or-rows options] (insert!* connectable tableable columns (one-or-many row-or-rows) (merge (conn/default-options connectable)
                                                                                                                                options))))

(m/defmulti insert-returning-keys!*
  {:arglists '([connectable tableable columns row-or-rows options])}
  u/dispatch-on-first-three-args)

(defn insert-returning-keys!
  ([tableable row-or-rows]                             (insert-returning-keys!* :current    tableable nil     row-or-rows nil))
  ([tableable columns row-or-rows]                     (insert-returning-keys!* :current    tableable columns row-or-rows nil))
  ([connectable tableable columns row-or-rows]         (insert-returning-keys!* connectable tableable columns row-or-rows nil))
  ([connectable tableable columns row-or-rows options] (insert-returning-keys!* connectable tableable columns row-or-rows options)))

(m/defmethod insert-returning-keys!* :default
  [connectable tableable columns row-or-rows options]
  (insert! connectable tableable columns row-or-rows (merge {:statement/return-generated-keys true}
                                                        options)))

(defn conditions->where-clause
  "Convert `conditions` to a HoneySQL-style WHERE clause vector if it is not already one (e.g., if it is a map)."
  [conditions]
  (cond
    (map? conditions)
    (let [clauses (for [[k v] conditions]
                    (if (vector? v)
                      (into [(first v) k] (rest v))
                      [:= k v]))]
      (if (> (bounded-count 2 clauses) 1)
        (into [:and] clauses)
        (first clauses)))

    (seq conditions)
    conditions))

(m/defmulti update!*
  {:arglists '([connectable tableable conditions changes options])}
  u/dispatch-on-first-two-args)

(m/defmethod update!* :default
  [connectable tableable conditions changes options]
  (let [honeysql-form (merge {:update (table-name connectable tableable)
                              :set    changes}
                             (when-let [where-clause (conditions->where-clause conditions)]
                               {:where where-clause}))]
    (query/execute! connectable tableable honeysql-form options)))

(defn update!
  "Convenience for updating row(s). `conditions` can be either a map of `{field value}` or a HoneySQL-style vector where
  clause. Returns number of rows updated.

    ;; UPDATE venues SET expensive = false WHERE price = 1
    (jdbc/update! conn :venues {:price 1} {:expensive false})

    ;; Same as above, but with HoneySQL-style vector conditions
    (jdbc/update! conn :venues [:= :price 1] {:expensive false})

    ;; To use an operator other than `:=`, wrap the value in a vector e.g. `[:operator & values]`
    ;; UPDATE venues SET expensive = false WHERE price BETWEEN 1 AND 2
    (jdbc/update! conn :venues {:price [:between 1 2]} {:expensive false})"
  ([tableable conditions changes]                     (update!* :default    tableable conditions changes nil))
  ([connectable tableable conditions changes]         (update!* connectable tableable conditions changes nil))
  ([connectable tableable conditions changes options] (update!* connectable tableable conditions changes options)))

(m/defmulti delete!*
  {:arglists '([connectable tableable conditions options])}
  u/dispatch-on-first-two-args)

(m/defmethod delete!* :default
  [connectable tableable conditions options]
  (let [honeysql-form (merge {:delete-from (table-name connectable tableable)}
                             (when-let [where-clause (conditions->where-clause conditions)]
                               {:where where-clause}))]
    (query/execute! connectable tableable honeysql-form options)))

(defn delete!
  ([object]                                   (delete!* :current    (instance/table object) (primary-key object) nil))
  ([tableable conditions]                     (delete!* :current    tableable               conditions           nil))
  ([connectable tableable conditions]         (delete!* connectable tableable               conditions           nil))
  ([connectable tableable conditions options] (delete!* connectable tableable               conditions           options)))

(m/defmulti upsert!*
  {:arglists '([connectable tableable conditions row options])}
  u/dispatch-on-first-two-args)

;; TODO

(defn upsert!
  ([tableable conditions row]                     (upsert!* :current    tableable conditions row nil))
  ([connectable tableable conditions row]         (upsert!* connectable tableable conditions row nil))
  ([connectable tableable conditions row options] (upsert!* connectable tableable conditions row options)))

(m/defmulti save!*
  {:arglists '([connectable tableable object options])}
  u/dispatch-on-first-two-args)

;; TODO

(defn save!
  ([object]                               (save!* :current    (instance/table object) object nil))
  ([connectable object]                   (save!* connectable (instance/table object) object nil))
  ([connectable object options]           (save!* connectable (instance/table object) object options))
  ([connectable tableable object options] (save!* connectable tableable               object options)))
