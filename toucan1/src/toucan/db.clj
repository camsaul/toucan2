(ns toucan.db
  "Helper functions for querying the DB and inserting or updating records using Toucan models."
  (:refer-clojure :exclude [count])
  (:require
   [methodical.core :as m]
   [potemkin :as p]
   [toucan.models :as models]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.delete :as delete]
   [toucan2.execute :as execute]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.model :as model]
   [toucan2.select :as select]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(comment models/keep-me)

(p/import-vars
 [models resolve-model]
 [select select select-one count select-reducible exists?])

(def ^:dynamic ^{:deprecated "2.0.0"} *quoting-style*
  "DEPRECATED: binding [[toucan2.compile/*honeysql-options*]] instead."
  nil)

(defn ^{:deprecated "2.0.0"} set-default-quoting-style!
  "DEPRECATED: set [[toucan2.compile/global-honeysql-options]] directly."
  [new-quoting-style]
  (swap! compile/global-honeysql-options assoc :dialect new-quoting-style, :quoted (boolean new-quoting-style)))

(defn ^{:deprecated "2.0.0"} quoting-style
  []
  (or *quoting-style*
      (get @compile/global-honeysql-options :dialect)))

(def ^:dynamic ^{:deprecated "2.0.0"} *automatically-convert-dashes-and-underscores*
  "DEPRECATED: binding [[toucan2.compile/*honeysql-options*]] instead."
  nil)

(defn ^{:deprecated "2.0.0"} set-default-automatically-convert-dashes-and-underscores!
  "DEPRECATED: set [[toucan2.compile/global-honeysql-options]] directly."
  [automatically-convert-dashes-and-underscores]
  (swap! compile/global-honeysql-options assoc :quoted-snake (boolean automatically-convert-dashes-and-underscores)))

(defn ^{:deprecated "2.0.0"} automatically-convert-dashes-and-underscores?
  []
  (if (nil? *automatically-convert-dashes-and-underscores*)
    (get @compile/global-honeysql-options :quoted-snake)
    *automatically-convert-dashes-and-underscores*))

(defn honeysql-options []
  (merge
   ;; defaults
   {:quoted true, :dialect :ansi, #_:quoted-snake #_true}
   compile/*honeysql-options*
   (when *quoting-style*
     {:dialect *quoting-style*})
   (when (some? *automatically-convert-dashes-and-underscores*)
     {:quoted-snake *automatically-convert-dashes-and-underscores*})))

(m/defmethod compile/do-with-compiled-query [:toucan1/model clojure.lang.IPersistentMap]
  [model honeysql f]
  (u/with-debug-result ["Compiling Honey SQL query for legacy Toucan 1 model %s" model]
    (binding [compile/*honeysql-options* (honeysql-options)]
      (next-method model honeysql f))))

;; replaces `*db-connection*`
(p/import-vars [current *connectable*])

(defn ^{:deprecated "2.0.0"} set-default-db-connection!
  "DEPRECATED: Implement [[toucan2.connection/do-with-connection]] for `:default` instead."
  [connectable]
  (m/defmethod conn/do-with-connection :toucan/default
    [_connectable f]
    (conn/do-with-connection connectable f)))

;; (defonce ^:private default-default-jdbc-options {:identifiers u/lower-case})

;; (defonce ^:private default-jdbc-options
;;   (atom default-default-jdbc-options))

(defn ^{:deprecated "2.0.0"} set-default-jdbc-options!
  "DEPRECATED: Set [[toucan2.jdbc.query/global-options]] directly instead."
  [jdbc-options]
  (swap! t2.jdbc.query/global-options jdbc-options))



;;;                                         TRANSACTION & CONNECTION UTIL FNS
;;; ==================================================================================================================

;; (def ^:dynamic *transaction-connection* nil)

(defn ^{:deprecated "2.0.0"} connection
  "DEPRECATED: use `:toucan2.connection/current` instead."
  []
  ::conn/current)

;; (defn do-in-transaction
;;   "Execute F inside a DB transaction. Prefer macro form `transaction` to using this directly."
;;   [f]
;;   (jdbc/with-db-transaction [conn (connection)]
;;     (binding [*transaction-connection* conn]
;;       (f))))

;; (defmacro transaction
;;   "Execute all queries within the body in a single transaction."
;;   {:arglists '([body] [options & body]), :style/indent 0}
;;   [& body]
;;   `(do-in-transaction (fn [] ~@body)))


(defmacro with-call-counting
  "DEPRECATED: Use [[toucan2.execute/with-call-count]] instead."
  {:deprecated "2.0.0", :style/indent 1}
  [[call-count-fn-binding] & body]
  `(execute/with-call-count [~call-count-fn-binding]
     ~@body))

(defmacro debug-print-queries
  "DEPRECATED: Bind [[toucan2.util/*debug*]] directly instead."
  {:deprecated "2.0.0", :style/indent 0}
  [& body]
  `(binding [u/*debug* true]
     ~@body))

(defn ^{:deprecated "2.0.0"} honeysql->sql
  "DEPRECATED: Use [[toucan2.compile/with-compiled-query]] instead."
  [honeysql-form]
  (compile/with-compiled-query [query [nil honeysql-form]]
    query))

(defn ^{:deprecated "2.0.0"} query
  "DEPRECATED: use [[toucan2.execute/query]] instead."
  [honeysql-form & {:as options}]
  (binding [t2.jdbc.query/*options* (merge t2.jdbc.query/*options* options)]
    (execute/query ::conn/current honeysql-form)))

(defn ^{:deprecated "2.0.0"} reducible-query
  "DEPRECATED: Use [[toucan2.execute/reducible-query]] instead."
  [honeysql-form & {:as options}]
  (reify clojure.lang.IReduceInit
    (reduce [_this rf init]
      (binding [t2.jdbc.query/*options* (merge t2.jdbc.query/*options* options)]
        (reduce rf init (execute/reducible-query ::conn/current honeysql-form))))))

(defn ^{:deprecated "2.0.0"} qualify
  ^clojure.lang.Keyword [modelable field-name]
  (if (vector? field-name)
    [(qualify modelable (first field-name)) (second field-name)]
    (model/with-model [model modelable]
      (keyword (str (name (model/table-name model)) \. (name field-name))))))

(defn ^{:deprecated "2.0.0"} qualified?
  "Is `field-name` qualified (e.g. with its table name)?"
  ^Boolean [field-name]
  (if (vector? field-name)
    (qualified? (first field-name))
    (boolean (re-find #"\." (name field-name)))))

(defn ^{:deprecated "2.0.0"} simple-select
  "DEPRECATED: Use [[toucan2.select/select]] instead."
  [modelable honeysql-form]
  (select/select modelable honeysql-form))

(defn ^{:deprecated "2.0.0"} simple-select-reducible
  "DEPRECATED: Use [[toucan2.select/select-reducible]] instead."
  [modelable honeysql-form]
  (select/select-reducible modelable honeysql-form))

(defn ^{:deprecated "2.0.0"} simple-select-one
  "DEPRECATED: use [[toucan2.select/select-one]] instead."
  ([modelable]
   (select/select-one modelable))
  ([modelable honeysql-form]
   (select/select-one modelable (assoc honeysql-form :limit 1))))

(defn ^{:deprecated "2.0.0"} execute!
  "DEPRECATED: use [[toucan2.execute/query-one]] instead."
  [honeysql-form & {:as options}]
  (binding [t2.jdbc.query/*options* (merge t2.jdbc.query/*options* options)]
    (execute/query-one honeysql-form)))

(defn update!
  "DEPRECATED: use [[toucan2.update/update!]] instead. The difference between this version and the Toucan 2 version is the
  key-value args are treated as conditions in Toucan 2; in Toucan 1 you generally would have had to
  use [[update-where!]] to update something without using the primary key. In Toucan 2, changes "
  {:deprecated "2.0.0", :arglists '([modelable honeysql-form]
                                    [modelable pk changes-map]
                                    [modelable pk key-to-change new-value & more])}
  ([modelable honeysql-form]
   (pos? (update/update! modelable honeysql-form)))

  ([modelable pk changes-map]
   (pos? (update/update! modelable pk changes-map)))

  ([modelable pk k v & {:as more}]
   (let [changes-map (merge {k v} more)]
     (update! modelable pk changes-map))))

(defn ^{:deprecated "2.0.0"} update-where!
  "DEPRECATED: use [[toucan2.update/update!]] instead."
  [modelable conditions-map & {:as changes}]
  (pos? (update/update! modelable conditions-map changes)))

(defn ^{:deprecated "2.0.0"} update-non-nil-keys!
  "DEPRECATED: there is currently no equivalent function in Toucan 2; use [[toucan2.update/update!]] and manually filter
  non-nil keys instead. This function may be added in the future."
  ([modelable id kvs]
   (update! modelable id (into {} (for [[k v] kvs
                                        :when (not (nil? v))]
                                    [k v]))))
  ([modelable id k v & more]
   (update-non-nil-keys! modelable id (apply array-map k v more))))


(defn- do-with-simple [model thunk]
  (binding [current/*connectable*                (if (= current/*connectable* :toucan/default)
                                                  (model/default-connectable model)
                                                  current/*connectable*)
            compile/*honeysql-options*          (honeysql-options)
            instance/*default-key-transform-fn* (instance/key-transform-fn model)]
    (thunk)))

(defn ^{:deprecated "2.0.0"} simple-insert-many!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead. Returns the ID of the "
  [modelable row-maps]
  (when (seq row-maps)
    (model/with-model [model modelable]
      (do-with-simple
       model
       (^:once fn* []
        (insert/insert-returning-pks! (model/table-name model) row-maps))))))

(defn ^{:deprecated "2.0.0"} insert-many!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead."
  [modelable row-maps]
  (when (seq row-maps)
    (model/with-model [_model modelable]
      (insert/insert-returning-pks! modelable row-maps))))

(defn ^{:deprecated "2.0.0"} simple-insert!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead."
  ([modelable row-map]
   (first (simple-insert-many! modelable [row-map])))

  ([modelable k v & more]
   (simple-insert! modelable (apply array-map k v more))))

(defn ^{:deprecated "2.0.0"} insert!
  "DEPRECATED: use [[toucan2.insert/insert-returning-instances!]] instead."
  {:style/indent 1}
  ([modelable row-map]
   (first (insert/insert-returning-instances! modelable row-map)))

  ([modelable k v & {:as more}]
   (insert! modelable (merge {k v} more))))

(defn ^{:deprecated "2.0.0"} select-one-field
  "DEPRECATED: Use [[toucan2.select/select-one-fn]] instead."
  [field modelable & options]
  (apply select/select-one-fn field modelable options))

(defn ^{:deprecated "2.0.0"} select-one-id
  "DEPRECATED: Use [[toucan2.select/select-one-pk]] instead."
  [modelable & options]
  (apply select/select-one-pk modelable options))

(defn ^{:deprecated "2.0.0"} select-field
  "DEPRECATED: Use [[toucan2.select/select-fn-set]] instead."
  [field modelable & options]
  (apply select/select-fn-set field modelable options))

(defn ^{:deprecated "2.0.0"} select-ids
  "DEPRECATED: Use [[toucan2.select/select-pk-set]] instead."
  [modelable & options]
  (apply select/select-pks-set modelable options))

(defn ^{:deprecated "2.0.0"} select-field->field
  "DEPRECATED: use [[toucan2.select/select-fn->fn]] instead."
  [k v modelable & options]
  (apply select/select-fn->fn k v modelable options))

(defn ^{:deprecated "2.0.0"} select-field->id
  "DEPRECATED: use [[toucan2.select/select-fn->pk]] instead."
  {:style/indent 2}
  [field modelable & options]
  (apply select/select-fn->pk field modelable options))

(defn ^{:deprecated "2.0.0"} select-id->field
  "DEPRECATED: use [[toucan2.select/select-pk->fn]] instead."
  {:style/indent 2}
  [field modelable & options]
  (apply select/select-pk->fn field modelable options))

;;; Sort of weird that this accepts a different args syntax than [[delete!]] does.
(defn ^{:deprecated "2.0.0"} simple-delete!
  "DEPRECATED: use [[toucan2.delete/delete!]] with a plain table name instead."
  ([modelable]
   (simple-delete! modelable nil))

  ([modelable conditions-map]
   (model/with-model [model modelable]
     (do-with-simple
      model
      (^:once fn* []
       (pos? (apply delete/delete! (model/table-name model) (into [] (mapcat vec) conditions-map)))))))

  ([modelable k v & {:as more}]
   (simple-delete! modelable (merge {k v} more))))

(defn ^{:deprecated "2.0.0"} delete!
  "DEPRECATED: use [[toucan2.delete/delete!]] instead."
  {:style/indent 1}
  [modelable & conditions]
  (pos? (apply delete/delete! modelable conditions)))
