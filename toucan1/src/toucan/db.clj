(ns toucan.db
  "Helper functions for querying the DB and inserting or updating records using Toucan models."
  (:refer-clojure :exclude [count])
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.set :as set]
   [clojure.spec.alpha :as s]
   [honey.sql :as hsql]
   [methodical.core :as m]
   [potemkin :as p]
   [pretty.core :as pretty]
   [toucan.models :as t1.models]
   [toucan2.connection :as conn]
   [toucan2.delete :as delete]
   [toucan2.execute :as execute]
   [toucan2.insert :as insert]
   [toucan2.jdbc :as jdbc]
   [toucan2.log :as log]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment t1.models/keep-me)

(p/import-vars
 [t1.models resolve-model])

(def ^:dynamic *quoting-style*
  "Temporarily override the default [[quoting-style]]. DEPRECATED: bind [[toucan2.map-backend.honeysql/*options*]]
  instead."
  nil)

(defn set-default-quoting-style!
  "Set the default [[quoting-style]]. DEPRECATED: set [[toucan2.map-backend.honeysql2/global-options]] directly."
  [new-quoting-style]
  (swap! map.honeysql/global-options assoc :dialect new-quoting-style, :quoted (boolean new-quoting-style)))

(defn quoting-style
  "In Toucan 1, this was the `:quoting` option to pass to Honey SQL 1. This now corresponds to the `:dialect` option
  passed to Honey SQL 2."
  []
  (or *quoting-style*
      (get @map.honeysql/global-options :dialect)))

(def ^:dynamic *automatically-convert-dashes-and-underscores*
  "Whether to automatically convert dashes in keywords to `snake_case` when compiling HoneySQL queries, even when quoting,
  and to convert underscores in result column names to dashes (i.e., convert to `lisp-case`). This is `false` by
  default.

  DEPRECATED: bind [[toucan2.map-backend.honeysql/*options*]] and [[toucan2.jdbc/*options*]] instead."
  nil)

;;; TODO -- this is NOT TESTED ANYWHERE !!!!
(defn set-default-automatically-convert-dashes-and-underscores!
  "DEPRECATED: set [[toucan2.map-backend.honeysql2/global-options]] directly."
  [automatically-convert-dashes-and-underscores]
  (swap! map.honeysql/global-options assoc :quoted-snake (boolean automatically-convert-dashes-and-underscores))
  (if automatically-convert-dashes-and-underscores
    (swap! jdbc/global-options assoc :label-fn csk/->kebab-case)
    (swap! jdbc/global-options dissoc :label-fn)))

(defn automatically-convert-dashes-and-underscores?
  []
  (if (nil? *automatically-convert-dashes-and-underscores*)
    (get @map.honeysql/global-options :quoted-snake)
    *automatically-convert-dashes-and-underscores*))

(defn- honeysql-options []
  (merge
   map.honeysql/*options*
   (when-let [style *quoting-style*]
     {:dialect style})
   (when (some? *automatically-convert-dashes-and-underscores*)
     {:quoted-snake *automatically-convert-dashes-and-underscores*})))

(defn set-default-jdbc-options!
  "DEPRECATED: Set [[toucan2.jdbc.query/global-options]] directly instead."
  [jdbc-options]
  (swap! jdbc/global-options merge (merge
                                    ;; apparently if you don't set `:identifiers` we're supposed to be defaulting
                                    ;; to [[u/lower-case-en]]
                                    {:label-fn u/lower-case-en}
                                    (set/rename-keys jdbc-options {:identifiers :label-fn}))))

(defn- jdbc-options []
  (merge
   (when *automatically-convert-dashes-and-underscores*
     {:label-fn u/->kebab-case})
   jdbc/*options*))

(m/defmethod pipeline/transduce-query [#_query-type :default #_model :toucan1/model #_resolved-query :default]
  [rf query-type model parsed-args resolved-query]
  (log/debugf :compile "Compiling Honey SQL query for legacy Toucan 1 model %s" model)
  (binding [map.honeysql/*options* (honeysql-options)
            jdbc/*options*         (jdbc-options)]
    (next-method rf query-type model parsed-args resolved-query)))

;; replaces `*db-connection*`
(p/import-vars [conn *current-connectable*])

(defn set-default-db-connection!
  "DEPRECATED: Implement [[toucan2.connection/do-with-connection]] for `:default` instead."
  [connectable]
  (m/defmethod conn/do-with-connection :default
    [_connectable f]
    (conn/do-with-connection connectable f)))



;;;                                         TRANSACTION & CONNECTION UTIL FNS
;;; ==================================================================================================================

(defmacro transaction
  "DEPRECATED: use [[toucan2.connection/with-connection]] instead."
  {:style/indent 0}
  [& body]
  `(conn/with-transaction [~'&transaction-connection nil]
     ~@body))

(defn quote-fn
  []
  (or (some-> (honeysql-options) :dialect hsql/get-dialect :quote)
      identity))

(defmacro with-call-counting
  "DEPRECATED: Use [[toucan2.execute/with-call-count]] instead."
  {:style/indent 1}
  [[call-count-fn-binding] & body]
  `(execute/with-call-count [~call-count-fn-binding]
     ~@body))

(defmacro debug-print-queries
  "DEPRECATED: Bind [[toucan2.util/*debug*]] directly instead."
  {:style/indent 0}
  [& body]
  `(binding [log/*level* :debug]
     ~@body))

(defn honeysql->sql
  "DEPRECATED: Use [[toucan2.pipeline/compile*]] instead."
  [honeysql-form]
  (binding [map.honeysql/*options* (honeysql-options)]
    (pipeline/compile* honeysql-form)))

;;; TODO -- are we sure we need to do things this way? Can't this stuff be bound in a pipeline method?
(deftype ^:no-doc Toucan1ReducibleQuery [honeysql-form query-jdbc-options]
  clojure.lang.IReduce
  (reduce [this rf]
    (reduce rf (rf) this))

  clojure.lang.IReduceInit
  (reduce [this rf init]
    (log/debugf :results "reduce Toucan 1 reducible query %s" this)
    (binding [jdbc/*options*         (merge jdbc/*options* query-jdbc-options)
              map.honeysql/*options* (honeysql-options)]
      (reduce ((map realize/realize) rf) init (execute/reducible-query nil honeysql-form))))

  Object
  (equals [_this another]
    (and (instance? Toucan1ReducibleQuery another)
         (= (.honeysql_form ^Toucan1ReducibleQuery another) honeysql-form)
         (= (.query_jdbc_options ^Toucan1ReducibleQuery another) query-jdbc-options)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->Toucan1ReducibleQuery honeysql-form query-jdbc-options)))

(defn reducible-query
  "DEPRECATED: Use [[toucan2.execute/reducible-query]] instead."
  [honeysql-form & {:as query-jdbc-options}]
  (->Toucan1ReducibleQuery honeysql-form query-jdbc-options))

(defn query
  "DEPRECATED: use [[toucan2.execute/query]] instead."
  [honeysql-form & query-jdbc-options]
  (realize/realize (apply reducible-query honeysql-form query-jdbc-options)))

(defn qualify
  ^clojure.lang.Keyword [modelable field-name]
  (if (vector? field-name)
    [(qualify modelable (first field-name)) (second field-name)]
    (let [model (model/resolve-model modelable)]
      (keyword (str (name (model/table-name model)) \. (name field-name))))))

(defn qualified?
  "Is `field-name` qualified (e.g. with its table name)?"
  ^Boolean [field-name]
  (if (vector? field-name)
    (qualified? (first field-name))
    (boolean (re-find #"\." (name field-name)))))

;; (defn do-post-select
;;   "DEPRECATED: You almost certainly don't need to be using this -- us [[toucan2.select/select]] instead, which can now
;;   handle arbitrary queries."
;;   [modelable rows]
;;   (let [model (model/resolve-model modelable)]
;;     (select/select model (identity-query/identity-query rows))))

(s/def ::select-options
  (s/* (s/alt :kvs (s/+ (s/cat :k keyword?
                               :v any?))
              :map map?)))

(defn- parse-select-options [options]
  (let [parsed (s/conform ::select-options options)]
    (when (s/invalid? parsed)
      (throw (ex-info (str "Error parsing select options: " (s/explain-str ::select-options options))
                      (s/explain-data ::select-options options))))
    (transduce
     identity
     (fn
       ([] {:kvs []})
       ([{:keys [kvs m]}]
        (concat kvs (when m [m])))
       ([options [arg-type arg]]
        (case arg-type
          :map (update options :m merge arg)
          :kvs (update options :kvs concat (mapcat (juxt :k :v) arg)))))
     parsed)))

(defn simple-select
  "DEPRECATED: Use [[toucan2.select/select]] instead."
  [modelable honeysql-form]
  (binding [default-fields/*skip-default-fields* true]
    (select/select modelable honeysql-form)))

(defn simple-select-reducible
  "DEPRECATED: Use [[toucan2.select/reducible-select]] instead."
  [modelable honeysql-form]
  (reify clojure.lang.IReduceInit
    (reduce [_this rf init]
      (binding [default-fields/*skip-default-fields* true]
        (reduce
         rf
         init
         (eduction
          (map realize/realize)
          (select/reducible-select modelable honeysql-form)))))))

(defn select-reducible
  "DEPRECATED: Use [[toucan2.select/reducible-select]] instead."
  {:arglists '([modelable & kv-args? query?]
               [[modelable & columns] & kv-args? query?])}
  [modelable & options]
  (eduction
   (map realize/realize)
   (apply select/reducible-select modelable (parse-select-options options))))

(defn simple-select-one
  "DEPRECATED: use [[toucan2.select/select-one]] instead."
  ([modelable]
   (select/select-one modelable))
  ([modelable honeysql-form]
   (select/select-one modelable (assoc honeysql-form :limit 1))))

(defn execute!
  "DEPRECATED: use [[toucan2.execute/query-one]] instead."
  [honeysql-form & {:as options}]
  (binding [jdbc/*options* (merge jdbc/*options* options)]
    (execute/query-one honeysql-form)))

(defn update!
  "DEPRECATED: use [[toucan2.update/update!]] instead. Returns `true` if anything was updated, `false` otherwise.

  Differences from Toucan 2 version:

  * 2-arity version takes a honeysql form. There is no direct equivalent in Toucan 2; use [[toucan2.execute/query]]
    instead.

  * key-value args are treated as conditions in Toucan 2; in Toucan 1 you generally would have had to
    use [[update-where!]] to update something without using the primary key."
  {:arglists '([modelable honeysql-form]
               [modelable pk changes-map]
               [modelable pk key-to-change new-value & more])}
  ([modelable honeysql-form]
   (let [model    (model/resolve-model modelable)
         honeysql (merge
                   {:update [(keyword (model/table-name model))]}
                   honeysql-form)]
     (pos? (execute/query-one nil :toucan.result-type/update-count modelable honeysql))))

  ([modelable pk changes-map]
   (pos? (update/update! modelable pk changes-map)))

  ([modelable pk k v & {:as more}]
   (let [changes-map (merge {k v} more)]
     (update! modelable pk changes-map))))

;;; wraps a model to prevent `before-update` and stuff like that from happening.
;;;
;;; TODO -- maybe this belongs in the main part of Toucan 2. You can just use the string table name directly for the
;;; most part (if you have a default connection defined) but in situations where the connections comes from the model
;;; this doesn't work.
;;;
;;; IMPORTANT! Make sure you resolve the model before you call `->SimpleModel`!
(defrecord ^:no-doc SimpleModel [original-model]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `->SimpleModel original-model))

  protocols/IModel
  (model [_this]
    original-model)

  protocols/IWithModel
  (with-model [_this new-model]
    (SimpleModel. new-model)))

(m/defmethod model/table-name SimpleModel
  [{:keys [original-model]}]
  (model/table-name original-model))

(m/defmethod model/default-connectable SimpleModel
  [{:keys [original-model]}]
  (model/default-connectable original-model))

(m/defmethod model/primary-keys SimpleModel
  [{:keys [original-model]}]
  (model/primary-keys original-model))

(m/defmethod pipeline/transduce-query [#_query-type :default #_model SimpleModel #_resolved-query :default]
  [rf query-type model parsed-args resolved-query]
  (binding [map.honeysql/*options* (honeysql-options)]
    (next-method rf query-type model parsed-args resolved-query)))

(defn update-where!
  "DEPRECATED: use [[toucan2.update/update!]] instead.

  This preserves Toucan 1 behavior and does a 'simple' update that does not do `pre-update`, transforms, or anything
  else like that. If you want a version without the weird gotchas, you can use [[toucan2.update/update!]] instead.

  Returns `true` if any rows were updated, `false` otherwise."
  [modelable conditions-map & {:as changes}]
  (let [model (model/resolve-model modelable)]
    (pos? (update/update! (->SimpleModel model) conditions-map changes))))

(defn update-non-nil-keys!
  "DEPRECATED: there is currently no equivalent function in Toucan 2; use [[toucan2.update/update!]] and manually filter
  non-nil keys instead. This function may be added in the future."
  ([modelable id kvs]
   (update! modelable id (into {} (for [[k v] kvs
                                        :when (not (nil? v))]
                                    [k v]))))
  ([modelable id k v & more]
   (update-non-nil-keys! modelable id (apply array-map k v more))))

(defn simple-insert-many!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead. Returns the IDs of the inserted rows."
  [modelable row-maps]
  (when (seq row-maps)
    (let [model (t1.models/resolve-model modelable)]
      (insert/insert-returning-pks! (->SimpleModel model) row-maps))))

(defn insert-many!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead."
  [modelable row-maps]
  (when (seq row-maps)
    (insert/insert-returning-pks! modelable row-maps)))

(defn simple-insert!
  "DEPRECATED: use [[toucan2.insert/insert-returning-pks!]] instead."
  ([modelable row-map]
   (first (simple-insert-many! modelable [row-map])))

  ([modelable k v & more]
   (simple-insert! modelable (apply array-map k v more))))

(defn insert!
  "DEPRECATED: use [[toucan2.insert/insert-returning-instances!]] instead."
  ([modelable row-map]
   (first (insert/insert-returning-instances! modelable row-map)))

  ([modelable k v & {:as more}]
   (insert! modelable (merge {k v} more))))

(defn select-one
  "DEPRECATED: use [[toucan2.select/select-one]] instead."
  [modelable & options]
  (apply select/select-one modelable (parse-select-options options)))

(defn select-one-field
  "DEPRECATED: Use [[toucan2.select/select-one-fn]] instead."
  [field modelable & options]
  (apply select/select-one-fn field modelable (parse-select-options options)))

(defn select-one-id
  "DEPRECATED: Use [[toucan2.select/select-one-pk]] instead."
  [modelable & options]
  (apply select/select-one-pk modelable (parse-select-options options)))

(defn count
  "DEPRECATED: use [[toucan2.select/count]] instead."
  [modelable & options]
  (apply select/count modelable (parse-select-options options)))

(defn select
  "DEPRECATED: use [[toucan2.select/select]] instead."
  [modelable & options]
  (apply select/select modelable (parse-select-options options)))

(defn select-field
  "DEPRECATED: Use [[toucan2.select/select-fn-set]] instead."
  [field modelable & options]
  (apply select/select-fn-set field modelable (parse-select-options options)))

(defn select-ids
  "DEPRECATED: Use [[toucan2.select/select-pk-set]] instead."
  [modelable & options]
  (apply select/select-pks-set modelable (parse-select-options options)))

(defn select-field->field
  "DEPRECATED: use [[toucan2.select/select-fn->fn]] instead."
  [k v modelable & options]
  (apply select/select-fn->fn k v modelable (parse-select-options options)))

(defn select-field->id
  "DEPRECATED: use [[toucan2.select/select-fn->pk]] instead."
  [field modelable & options]
  (apply select/select-fn->pk field modelable (parse-select-options options)))

(defn select-id->field
  "DEPRECATED: use [[toucan2.select/select-pk->fn]] instead."
  [field modelable & options]
  (apply select/select-pk->fn field modelable (parse-select-options options)))

;;; we could actually leverage Clojure 11 support for passing a map to `& {:keys ...}` and just have one arity, but we
;;; want to support older Clojure versions for now at least.
(defn exists?
  "DEPRECATED: use [[toucan2.select/exists?]] instead."
  ([modelable]
   (select/exists? modelable))
  ;; conditions maps weren't intentionally supported in Toucan 1, but they worked unintentionally, so continue to
  ;; support them.
  ([modelable conditions]
   (apply select/exists? modelable (mapcat identity conditions)))
  ([modelable k v & more]
   (apply select/exists? modelable k v more)))

;;; Sort of weird that this accepts a different args syntax than [[delete!]] does.
(defn simple-delete!
  "DEPRECATED: use [[toucan2.delete/delete!]] with a plain table name instead."
  ([modelable]
   (simple-delete! modelable nil))

  ([modelable conditions-map]
   (let [model (model/resolve-model modelable)]
     (pos? (apply delete/delete! (->SimpleModel model) (into [] (mapcat vec) conditions-map)))))

  ([modelable k v & {:as more}]
   (simple-delete! modelable (merge {k v} more))))

(defn delete!
  "DEPRECATED: use [[toucan2.delete/delete!]] instead."
  [modelable & conditions]
  (pos? (apply delete/delete! modelable conditions)))

(def ^:dynamic *disable-db-logging*
  "DEPRECATED: Toucan 2 does not currently have 'DB logging' to enable or disable, and even if it did, it's unlikely that
  we would have a with a dynamic var for toggling it. Instead. we'll probably use `clojure.tools.logging`. This is
  here mostly to minimize the number of changes you need to make to existing code. Binding it has no effect whatsoever."
  false)
