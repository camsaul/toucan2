(ns toucan2.mutative
  "Table-aware methods that for changing data in the database, e.g. `update!`, `insert!`, and `save!`."
  (:require [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.build-query :as build-query]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.instance :as instance]
            [toucan2.log :as log]
            [toucan2.query :as query]
            [toucan2.select :as select]
            [toucan2.specs :as specs]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

(m/defmulti parse-update!-args*
  {:arglists '([connectableᵈ tableableᵈ argsᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(s/def ::update!-args
  (s/cat :pk            (s/? ::specs/pk)
         :conditions    (s/? map?)
         :kv-conditions ::specs/kv-conditions
         :changes       map?
         :options       (s/? ::specs/options)))

(m/defmethod parse-update!-args* :default
  [_ _ args _]
  (let [parsed (s/conform ::update!-args args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret update! args: %s" (s/explain-str ::update!-args args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (let [{:keys [kv-conditions]} parsed]
      (-> parsed
          (dissoc :kv-conditions)
          (update :conditions merge (when (seq kv-conditions)
                                      (zipmap (map :k kv-conditions) (map :v kv-conditions))))))))

(m/defmethod parse-update!-args* :around :default
  [connectable tableable args options]
  (log/with-debug ["Parsing update! args for %s %s" tableable args]
    (next-method connectable tableable args options)))

(m/defmulti update!*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod update!* :default
  [connectable tableable query options]
  (query/execute! connectable tableable query options))

;; Syntax is a bit different from Toucan -- Toucan is either
;;
;;    (update! table id changes-map)
;;
;; or
;;
;;    (update! table id & {:as changes})
;;
;; (i.e., the key-value varargs are collected into changes) whereas this syntax collects the key-value varargs into
;; conditions and changes must always be a map.
;;
;; TODO -- update! should take a queryable HoneySQL arg, maybe we can figure this out by checking if `:where` is
;; present?
(defn parse-update-args [connectable-tableable args]
  (let [[connectable tableable]                 (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]                   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [pk conditions changes options]} (parse-update!-args* connectable tableable args options)
        conditions                              (if-not pk
                                                  conditions
                                                  (build-query/merge-primary-key connectable tableable conditions pk options))
        query                                   (when (seq changes)
                                                  (cond-> (build-query/maybe-buildable-query connectable tableable nil :update options)
                                                    true             (build-query/with-table* tableable options)
                                                    true             (build-query/with-changes* changes options)
                                                    (seq conditions) (build-query/merge-kv-conditions* conditions options)))]
    {:connectable connectable
     :tableable   tableable
     :query       query
     :options     options}))

(defn update!
  "Returns number of rows updated."
  {:arglists '([connectable-tableable pk? & conditions? changes options?])}
  [connectable-tableable & args]
  (let [{:keys [connectable tableable query options]} (parse-update-args connectable-tableable args)]
    (if (empty? query)
      (do
        (log/trace "Query has no changes, skipping update")
        0)
      (log/with-trace ["UPDATE %s SET %s WHERE %s" tableable (build-query/changes* query) (build-query/conditions* query)]
        (update!* connectable tableable query options)))))

(m/defmulti save!*
  {:arglists '([connectableᵈ tableableᵈ objᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod save!* :default
  [connectable tableable obj _]
  (log/with-trace ["Saving %s (changes: %s)" obj (instance/changes obj)]
    (if-let [changes (not-empty (instance/changes obj))]
      (let [pk-values     (tableable/primary-key-values connectable tableable obj)
            rows-affected (update! [connectable tableable] pk-values changes)]
        (when-not (pos? rows-affected)
          (throw (ex-info (format "Unable to save object: %s with primary key %s does not exist." tableable pk-values)
                          {:object obj
                           :pk     pk-values})))
        (when (> rows-affected 1)
          (log/warnf "Warning: more than 1 row affected when saving %s with primary key %s" tableable pk-values))
        (instance/reset-original obj))
      (do
        (log/tracef "No changes; nothing to save.")
        obj))))

(defn save!
  [obj]
  (let [tableable             (instance/tableable obj)
        connectable           (instance/connectable obj)
        [connectable options] (conn.current/ensure-connectable connectable tableable nil)]
    (save!* connectable tableable obj options)))

(m/defmulti insert!*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod insert!* :default
  [connectable tableable query options]
  (query/execute! connectable tableable query options))

(m/defmulti parse-insert!-args*
  {:arglists '([connectableᵈ tableableᵈ argsᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(s/def ::insert!-args
  (s/cat :rows (s/alt :single-row-map    map?
                      :multiple-row-maps (s/coll-of map?)
                      :kv-pairs          ::specs/kv-conditions
                      :columns-rows      (s/cat :columns (s/coll-of keyword?)
                                                :rows    (s/coll-of vector?)))
         :options (s/? ::specs/options)))

(m/defmethod parse-insert!-args* :default
  [_ _ args _]
  (let [parsed (s/conform ::insert!-args args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret insert! args: %s" (s/explain-str ::insert!-args args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (update parsed :rows (fn [[rows-type x]]
                           (condp = rows-type
                             :single-row-map    [x]
                             :multiple-row-maps x
                             :kv-pairs          [(into {} (map (juxt :k :v)) x)]
                             :columns-rows      (let [{:keys [columns rows]} x]
                                                  (for [row rows]
                                                    (zipmap columns row))))))))

(defn parse-insert-args [connectable-tableable args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        [connectable options]   (conn.current/ensure-connectable connectable tableable nil)
        {:keys [rows options]}  (parse-insert!-args* connectable tableable args options)
        query                   (when (seq rows)
                                  (-> (build-query/maybe-buildable-query connectable tableable nil :insert options)
                                      (build-query/with-table* tableable options)
                                      (build-query/with-rows* rows options)))]
    {:connectable connectable
     :tableable   tableable
     :query       query
     :options     options}))

(defn do-insert! [connectable tableable {:keys [values], :as query} options]
  (log/with-trace ["INSERT %d %s rows:\n%s" (count values) tableable (u/pprint-to-str values)]
    (try
      (assert (seq values) "Values cannot be empty")
      (insert!* connectable tableable query options)
      (catch Throwable e
        (throw (ex-info (format "Error in insert!: %s" (ex-message e))
                        {:tableable tableable, :query query, :options options}
                        e))))))

(defn insert!
  "Returns number of rows inserted."
  {:arglists '([connectable-tableable row-or-rows options?]
               [connectable-tableable k v & more options?]
               [connectable-tableable columns row-vectors options?])}
  [connectable-tableable & args]
  (let [{:keys [connectable tableable query options]} (parse-insert-args connectable-tableable args)]
    (if (empty? query)
      (do
        (log/trace "No rows to insert.")
        0)
      (do-insert! connectable tableable query options))))

(defn insert-returning-keys!
  {:arglists '([connectable-tableable row-or-rows options?]
               [connectable-tableable k v & more options?]
               [connectable-tableable columns row-vectors options?])}
  [connectable-tableable & args]
  (let [{:keys [connectable tableable query options]} (parse-insert-args connectable-tableable args)
        options                                       (-> options
                                                          (assoc-in [:next.jdbc :return-keys] true)
                                                          (assoc :reducible? true))
        reducible-query (do-insert! connectable tableable query options)]
    (into
     []
     (map (select/select-pks-fn connectable tableable))
     reducible-query)))

(m/defmulti parse-delete-args*
  {:arglists '([connectableᵈ tableableᵈ argsᵈᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-delete-args* :default
  [connectable tableable args options]
  (select/parse-select-args* connectable tableable args options))

(m/defmethod parse-delete-args* :around :default
  [connectable tableable args options]
  (log/with-trace ["Parsing delete! args for %s %s" tableable args]
    (next-method connectable tableable args options)))

(defn parse-delete-args [[connectable-tableable & args]]
  (let [[connectable tableable]            (conn/parse-connectable-tableable connectable-tableable)
        [connectable options-1]            (conn.current/ensure-connectable connectable tableable nil)
        {:keys [conditions query options]} (parse-delete-args* connectable tableable args options-1)
        options                            (u/recursive-merge options-1 options)
        query                              (cond-> (build-query/maybe-buildable-query connectable tableable query :delete options)
                                             true             (build-query/with-table* tableable options)
                                             (seq conditions) (build-query/merge-kv-conditions* conditions options))]
    {:connectable connectable
     :tableable   tableable
     :query       query
     :options     options}))

(m/defmulti delete!*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod delete!* :default
  [connectable tableable query options]
  (let [query (build-query/maybe-buildable-query connectable tableable query :delete options)]
    (log/with-trace ["DELETE rows: %s" query]
      (query/execute! connectable tableable query options))))

(defn delete!
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (let [{:keys [connectable tableable query options]} (parse-delete-args args)]
    (delete!* connectable tableable query options)))

;; TODO
#_(defn upsert! [])
