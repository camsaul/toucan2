(ns bluejdbc.mutative
  "Table-aware methods that for changing data in the database, e.g. `update!`, `insert!`, and `save!`."
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as rs]
            [bluejdbc.select :as select]
            [bluejdbc.specs :as specs]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(m/defmulti parse-update!-args*
  {:arglists '([connectable tableable args options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-update!-args* :default
  [_ _ args _]
  (let [parsed (s/conform ::specs/update!-args args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret update! args: %s" (s/explain-str ::specs/update!-args args))
                      {:args args})))
    (log/tracef "-> %s" (u/pprint-to-str parsed))
    (let [{:keys [kv-conditions]} parsed]
      (-> parsed
          (dissoc :kv-conditions)
          (update :conditions merge (when (seq kv-conditions)
                                      (zipmap (map :k kv-conditions) (map :v kv-conditions))))))))

(m/defmethod parse-update!-args* :around :default
  [connectable tableable args options]
  (log/with-trace ["Parsing update! args for %s %s" tableable args]
    (next-method connectable tableable args options)))

(m/defmulti update!*
  {:arglists '([connectable tableable honeysql-form options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(defn- execute-results [results options]
  (if (get-in options [:execute :return-keys])
    results
    (first results)))

(m/defmethod update!* :default
  [connectable tableable honeysql-form options]
  (-> (query/execute! connectable tableable honeysql-form options)
      (execute-results options)))

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
(defn update!
  {:arglists '([connectable-tableable pk? & conditions? changes options?])}
  [connectable-tableable & args]
  (let [[connectable tableable]                 (conn/parse-connectable-tableable connectable-tableable)
        {:keys [pk conditions changes options]} (parse-update!-args* connectable tableable args (conn/default-options connectable))
        conditions                              (cond-> conditions
                                                  pk (honeysql-util/merge-primary-key connectable tableable pk options))
        honeysql-form                           (cond-> {:update (compile/table-identifier tableable options)
                                                         :set    changes}
                                                  (seq conditions) (honeysql-util/merge-conditions
                                                                    connectable tableable conditions options))]
    (log/tracef "UPDATE %s SET %s WHERE %s" (pr-str tableable) (pr-str changes) (pr-str conditions))
    (update!* connectable tableable honeysql-form (merge (conn/default-options connectable)
                                                         options))))


(m/defmulti save!*
  {:arglists '([connectable tableable obj options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod save!* :default
  [connectable tableable obj _]
  (when-let [changes (not-empty (instance/changes obj))]
    (update! [connectable tableable] (tableable/primary-key-values connectable tableable obj) changes)))

(defn save!
  [obj]
  (let [connectable (or (instance/connectable obj) conn.current/*current-connectable*)
        tableable   (instance/table obj)
        options     (conn/default-options connectable)]
    (save!* connectable tableable obj options)))

(m/defmulti insert!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod insert!* :default
  [connectable tableable query options]
  (-> (query/execute! connectable tableable query options)
      (execute-results options)))

(m/defmulti parse-insert!-args*
  {:arglists '([connectable tableable args options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-insert!-args* :default
  [_ _ args _]
  (let [parsed (s/conform ::specs/insert!-args args)]
    (when (= parsed :clojure.spec.alpha/invalid)
      (throw (ex-info (format "Don't know how to interpret insert! args: %s" (s/explain-str ::specs/insert!-args args))
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

(defn insert!
  {:arglists '([connectable-tableable row-or-rows options?]
               [connectable-tableable k v & more options?]
               [connectable-tableable columns row-vectors options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        {:keys [rows options]}  (parse-insert!-args* connectable tableable args (conn/default-options connectable))
        honeysql-form           {:insert-into (compile/table-identifier tableable options)
                                 :values      rows}]
    (log/tracef "INSERT %d %s rows:\n%s" (count rows) (pr-str tableable) (u/pprint-to-str rows))
    (insert!* connectable tableable honeysql-form options)))

(defn insert-returning-keys!
  {:arglists '([connectable-tableable row-or-rows options?]
               [connectable-tableable k v & more options?]
               [connectable-tableable columns row-vectors options?])}
  [connectable-tableable & args]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        {:keys [rows options]}  (parse-insert!-args* connectable tableable args (conn/default-options connectable))
        pks                     (tableable/primary-key-keys connectable tableable)
        get-pks                 (if (= (count pks) 1)
                                  (first pks)
                                  (apply juxt pks))
        options                 (u/recursive-merge
                                 options
                                 {:execute {:return-keys true
                                            :builder-fn  (rs/row-builder-fn connectable tableable)}})
        results                 (insert! [connectable tableable] rows options)]
    (map get-pks results)))

(m/defmulti parse-delete-args*
  {:arglists '([connectable tableable args options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod parse-delete-args* :default
  [connectable tableable args options]
  (select/parse-select-args connectable tableable args options))

(m/defmethod parse-delete-args* :around :default
  [connectable tableable args options]
  (log/with-trace ["Parsing delete! args for %s %s" tableable args]
    (next-method connectable tableable args options)))

(defn parse-delete-args [[connectable-tableable & args]]
  (let [[connectable tableable] (conn/parse-connectable-tableable connectable-tableable)
        {:keys [query options]} (parse-delete-args* connectable tableable args (conn/default-options connectable))]
    {:connectable connectable
     :tableable   tableable
     :query       query
     :options     options}))

(m/defmulti delete!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod delete!* :default
  [connectable tableable query options]
  (let [honeysql-form (merge {:delete-from (compile/table-identifier tableable options)}
                             query)]
    (log/with-trace ["DELETE rows: %s" honeysql-form]
      (query/execute! connectable tableable honeysql-form options))))

(defn delete!
  {:arglists '([connectable-tableable pk? & conditions? queryable? options?])}
  [& args]
  (let [{:keys [connectable tableable query options]} (parse-delete-args args)]
    (execute-results (delete!* connectable tableable query options) options)))

;; TODO
#_(defn upsert! [])
