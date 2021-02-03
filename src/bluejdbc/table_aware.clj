(ns bluejdbc.table-aware
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.instance :as instance]
            [bluejdbc.query :as query]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [methodical.core :as m]))

;; TODO - consider whether there's any possible reason in the whole world to have this also dispatch on connection
;;
;; TODO -- this should work with objects as well
;;
;; TODO -- is this supposed to return a String or a Keyword? Need to document.
;;
;; TODO -- rename to tableable (?)
(m/defmulti table-name
  {:arglists '([table])}
  u/dispatch-on-first-arg)

(m/defmethod table-name :default
  [this]
  this)

(m/defmethod table-name String
  [s]
  s)

;; NOCOMMIT
(defn tableable? [x]
  (or (keyword? x)
      (boolean (m/effective-method (m/remove-primary-method table-name :default) (class x)))))

(defn- raw-identifier [identifier options]
  (hsql/raw (compile/quote-identifier identifier options)))

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
  {:arglists '([connectable table query options])}
  u/dispatch-on-first-three-args)

(m/defmethod select* [:default :default nil]
  [connectable table _ options]
  (select* connectable table {} options))

(m/defmethod select* [:default :default clojure.lang.IPersistentMap]
  [connectable table honeysql-form options]
  (let [honeysql-form (merge {:select [:*]
                              :from   [(table-name table)]}
                             honeysql-form)]
    (query/query connectable honeysql-form options)))

;; TODO -- what about stuff like (db/select Table :id 100)?
(defn select
  ([table]                           (select* :default    table nil   nil))
  ([table query]                     (select* :default    table query nil))
  ([connectable table query]         (select* connectable table query nil))
  ([connectable table query options] (select* connectable table query options)))

(m/defmulti select-one*
  {:arglists '([connectable table query options])}
  u/dispatch-on-first-three-args)

(m/defmethod select-one* [:default :default nil]
  [connectable table _ options]
  (select-one* connectable table {} options))

(m/defmethod select-one* [:default :default clojure.lang.IPersistentMap]
  [connectable table honeysql-form options]
  (let [honeysql-form (merge {:select [:*]
                              :from   [(table-name table)]}
                             honeysql-form)]
    (query/query-one connectable honeysql-form options)))

(defn select-one
  ([table]                           (select-one* :default    table nil   nil))
  ([table query]                     (select-one* :default    table query nil))
  ([connectable table query]         (select-one* connectable table query nil))
  ([connectable table query options] (select-one* connectable table query options)))

;; TODO -- a common method for saving or inserting stuff?

(m/defmulti insert!*
  {:arglists '([connectable table columns rows options])}
  u/dispatch-on-first-two-args)

(m/defmethod insert!* :default
  [connectable table columns rows options]
  (query/try-catch-when-include-queries-in-exceptions-enabled
    (let [honeysql-form (merge {:insert-into (raw-identifier (table-name table) options)
                                :values      rows}
                               (when (seq columns)
                                 {:columns (mapv #(raw-identifier % options)
                                                 columns)}))]
      (query/try-catch-when-include-queries-in-exceptions-enabled
        (query/execute! connectable honeysql-form options)
        (catch Throwable e
          (throw (ex-info "Error inserting rows"
                          {:honeysql-form honeysql-form}
                          e)))))
    (catch Throwable e
      (throw (ex-info "Error inserting rows"
                      {:table table, :columns columns, :rows (take 10 rows), :options options}
                      e)))))

(defn- one-or-many [row-or-rows]
  (cond
    (sequential? row-or-rows) row-or-rows
    (nil? row-or-rows)        nil
    :else                     [row-or-rows]))

;; TODO - can we make this argslist [connectable? table columns? row-or-rows options?]

(defn insert!
  ([table row-or-rows]                             (insert!* :default    table nil     (one-or-many row-or-rows) nil))
  ([table columns row-or-rows]                     (insert!* :default    table columns (one-or-many row-or-rows) nil))
  ([connectable table columns row-or-rows]         (insert!* connectable table columns (one-or-many row-or-rows) nil))
  ([connectable table columns row-or-rows options] (insert!* connectable table columns (one-or-many row-or-rows) options)))

(m/defmulti insert-returning-keys!*
  {:arglists '([connectable table columns row-or-rows options])}
  u/dispatch-on-first-three-args)

(defn insert-returning-keys!
  ([table row-or-rows]                             (insert-returning-keys!* :default    table nil     row-or-rows nil))
  ([table columns row-or-rows]                     (insert-returning-keys!* :default    table columns row-or-rows nil))
  ([connectable table columns row-or-rows]         (insert-returning-keys!* connectable table columns row-or-rows nil))
  ([connectable table columns row-or-rows options] (insert-returning-keys!* connectable table columns row-or-rows options)))

(m/defmethod insert-returning-keys!* :default
  [connectable table columns row-or-rows options]
  (insert! connectable table columns row-or-rows (merge {:statement/return-generated-keys true}
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
  {:arglists '([connectable table conditions changes options])}
  u/dispatch-on-first-two-args)

(m/defmethod update!* :default
  [connectable table conditions changes options]
  (let [honeysql-form (merge {:update (table-name table)
                              :set    changes}
                             (when-let [where-clause (conditions->where-clause conditions)]
                               {:where where-clause}))]
    (query/execute! connectable honeysql-form options)))

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
  ([table conditions changes]                     (update!* :default    table conditions changes nil))
  ([connectable table conditions changes]         (update!* connectable table conditions changes nil))
  ([connectable table conditions changes options] (update!* connectable table conditions changes options)))

(m/defmulti delete!*
  {:arglists '([connectable table conditions options])}
  u/dispatch-on-first-two-args)

(m/defmethod delete!* :default
  [connectable table conditions options]
  (let [honeysql-form (merge {:delete-from (table-name table)}
                             (when-let [where-clause (conditions->where-clause conditions)]
                               {:where where-clause}))]
    (query/execute! connectable honeysql-form options)))

(defn delete!
  ([object]                               (delete!* :default    (instance/table object) (primary-key object) nil))
  ([table conditions]                     (delete!* :default    table                   conditions           nil))
  ([connectable table conditions]         (delete!* connectable table                   conditions           nil))
  ([connectable table conditions options] (delete!* connectable table                   conditions           options)))

(m/defmulti upsert!*
  {:arglists '([connectable table conditions row options])}
  u/dispatch-on-first-two-args)

;; TODO

(defn upsert!
  ([table conditions row]                     (upsert!* :default    table conditions row nil))
  ([connectable table conditions row]         (upsert!* connectable table conditions row nil))
  ([connectable table conditions row options] (upsert!* connectable table conditions row options)))

(m/defmulti save!*
  {:arglists '([connectable object options])}
  (fn [connectable object _]
    [(u/keyword-or-class connectable) (instance/table object)]))

;; TODO

(defn save!
  ([object])
  ([connectable object])
  ([connectable object options]))
