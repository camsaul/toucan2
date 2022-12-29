(ns toucan2.pipeline
  "This is a low-level namespace implementing our query execution pipeline. Most of the stuff you'd use on a regular basis
  are implemented on top of stuff here.

  Pipeline order is

  1. [[transduce-unparsed]]
  2. [[transduce-parsed]]
  3. [[transduce-with-model]]
  4. [[transduce-resolve]]
  5. [[transduce-build]]
  6. [[transduce-compile]]
  7. [[transduce-execute]]
  8. [[transduce-execute-with-connection]]"
  (:refer-clojure :exclude [compile])
  (:require
   [methodical.core :as m]
   [methodical.util.trace :as m.trace]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.jdbc.query :as jdbc.query]
   [toucan2.log :as log]
   [toucan2.map-backend :as map]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

;;; the query type hierarchy below is used for pipeline methods and tooling to decide what sort of things they need to
;;; do -- for example you should not do row-map transformations to a query that returns an update count.

(derive :toucan.query-type/select.* :toucan.query-type/*)
(derive :toucan.query-type/insert.* :toucan.query-type/*)
(derive :toucan.query-type/update.* :toucan.query-type/*)
(derive :toucan.query-type/delete.* :toucan.query-type/*)

;;; `DML` (Data manipulation language) here means things like `UPDATE`, `DELETE`, or `INSERT`. Some goofballs include
;;; `SELECT` in this category, but we are not! We are calling `SELECT` a `DQL` (Data Query Language) statement. There
;;; are other types of queries like `DDL` (Data Definition Language, e.g. `CREATE TABLE`), but Toucan 2 doesn't
;;; currently have any tooling around those. Stuff like [[toucan2.execute/query]] that could potentially execute those
;;; don't care what kind of query you're executing anyway.

(derive :toucan.statement-type/DML :toucan.statement-type/*)
(derive :toucan.statement-type/DQL :toucan.statement-type/*)

(derive :toucan.query-type/select.* :toucan.statement-type/DQL)
(derive :toucan.query-type/insert.* :toucan.statement-type/DML)
(derive :toucan.query-type/update.* :toucan.statement-type/DML)
(derive :toucan.query-type/delete.* :toucan.statement-type/DML)

(derive :toucan.result-type/instances    :toucan.result-type/*)
(derive :toucan.result-type/pks          :toucan.result-type/*)
(derive :toucan.result-type/update-count :toucan.result-type/*)

(doto :toucan.query-type/select.instances
  (derive :toucan.query-type/select.*)
  (derive :toucan.result-type/instances))

(doto :toucan.query-type/insert.update-count
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/insert.pks
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/insert.instances
  (derive :toucan.query-type/insert.*)
  (derive :toucan.result-type/instances))

(doto :toucan.query-type/update.update-count
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/update.pks
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/update.instances
  (derive :toucan.query-type/update.*)
  (derive :toucan.result-type/instances))

(doto :toucan.query-type/delete.update-count
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/update-count))

(doto :toucan.query-type/delete.pks
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/pks))

(doto :toucan.query-type/delete.instances
  (derive :toucan.query-type/delete.*)
  (derive :toucan.result-type/instances))

(defn query-type?
  "True if `query-type` derives from one of the various abstract query keywords such as `:toucan.result-type/*` or
  `:toucan.query-type/*`. This does not guarantee that the query type is a 'concrete', just that it is something with
  some sort of query type information."
  [query-type]
  (some (fn [abstract-type]
          (isa? query-type abstract-type))
        [:toucan.result-type/*
         :toucan.query-type/*
         :toucan.statement-type/*]))


;;;; pipeline

(def ^:dynamic *call-count-thunk*
  "Thunk function to call every time a query is executed if [[toucan2.execute/with-call-count]] is in use. Implementees
  of [[transduce-execute-with-connection]] should invoke this every time a query gets executed. You can
  use [[increment-call-count!]] to simplify the chore of making sure it's non-`nil` before invoking it."
  nil)

(defn increment-call-count! []
  (when *call-count-thunk* (*call-count-thunk*)))

;;; The little subscripts below are so you can tell at a glance which args are used for dispatch.

(defn- dispatch-ignore-rf [f]
  (fn [_rf & args]
    (apply f args)))

;;;; [[transduce-execute-with-connection]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-execute-with-connection
  "The eighth and final step in the query execution pipeline. Called with a fully compiled query that can be executed
  natively, and an open connection for executing it.

  ```
  transduce-execute
  ↓
  toucan2.connection/with-connection
  ↓
  transduce-execute-with-connection ← YOU ARE HERE
  ↓
  execute query
  ↓
  transduce results
  ```

  Implementations should execute the query and transduce the results using `rf`. Implementations are database-specific,
  which is why this method dispatches off of connection type (e.g. `java.sql.Connection`). The default JDBC backend
  executes the query and reduces results with [[toucan2.jdbc.query/reduce-jdbc-query]]."
  {:arglists '([rf conn₁ query-type₂ model₃ compiled-query])}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

(m/defmethod transduce-execute-with-connection :around :default
  [rf conn query-type model compiled-query]
  {:pre [(ifn? rf) (some? conn) (query-type? query-type) (some? compiled-query)]}
  (log/tracef :execute
              "In %s with dispatch value %s"
              `transduce-execute-with-connection
              (m/dispatch-value transduce-execute-with-connection rf conn query-type model compiled-query))
  (log/debugf :execute "transduce query with %s connection" (class conn))
  (u/try-with-error-context ["with connection" {:connection (class conn)}]
    (next-method rf conn query-type model compiled-query)))

;;;; [[transduce-execute]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-execute
  "The seventh step in the query execution pipeline. Called with a compiled query that is ready to be executed natively, for
  example a `[sql & args]` vector, immediately before opening a connection.

  ```
  transduce-compile
  ↓
  (compile query)
  ↓
  transduce-execute ← YOU ARE HERE
  ↓
  toucan2.connection/with-connection
  ↓
  transduce-execute-with-connection
  ```

  The default implementation opens a connection (or uses the current connection)
  using [[toucan2.connection/with-connection]] and then calls [[transduce-execute-with-connection]]."
  {:arglists '([rf query-type₁ model₂ compiled-query₃])}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

(m/defmethod transduce-execute :around :default
  [rf query-type model compiled-query]
  {:pre [(ifn? rf) (query-type? query-type) (some? compiled-query)]}
  (log/tracef :execute
              "In %s with dispatch value %s"
              `transduce-execute
              (m/dispatch-value transduce-execute rf query-type model compiled-query))
  (log/debugf :execute "Execute %s" compiled-query)
  (u/try-with-error-context ["with compiled query" {:compiled-query compiled-query}]
    (next-method rf query-type model compiled-query)))

(defn- current-connectable [model]
  (or conn/*current-connectable*
      (model/default-connectable model)))

(m/defmethod transduce-execute :default
  [rf query-type model compiled-query]
  (conn/with-connection [conn (current-connectable model)]
    (transduce-execute-with-connection rf conn query-type model compiled-query)))

;;; For DML stuff we will run the whole thing in a transaction if we're not already in one.
;;;
;;; Not 100% sure this is necessary since we would probably already be in one if we needed to be because stuff like
;;; [[toucan2.tools.before-delete]] have to put us in one much earlier.
(m/defmethod transduce-execute [#_query-type :toucan.statement-type/DML #_model :default #_compiled-query :default]
  [rf query-type model compiled-query]
  (conn/with-transaction [_conn (current-connectable model) {:nested-transaction-rule :ignore}]
    (next-method rf query-type model compiled-query)))

;;;; [[transduce-compile]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-compile
  "The sixth step in the query execution pipeline. Called with a query that is ready to be compiled, e.g. a fully-formed
  Honey SQL form.

  ```
  transduce-build
  ↓
  (build query)
  ↓
  transduce-compile ← YOU ARE HERE
  ↓
  (compile query)
  ↓
  transduce-execute
  ```

  The default implementation compiles the query and then calls [[transduce-execute]]."
  {:arglists '([rf query-type₁ model₂ built-query₃])}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

;;; HACK
(def ^:private ^:dynamic *built-query* nil)

(m/defmethod transduce-compile :around :default
  [rf query-type model built-query]
  {:pre [(ifn? rf) (query-type? query-type) (some? built-query)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-compile
              (m/dispatch-value transduce-compile rf query-type model built-query))
  (log/debugf :compile "Compile %s" built-query)
  (u/try-with-error-context ["with built query" {:query-type query-type, :built-query built-query}]
    ;; keep the original info around as metadata in case someone needs it later (they will)
    (binding [*built-query* built-query]
      (next-method rf query-type model built-query))))

(m/defmethod transduce-compile :default
  [rf query-type model query]
  (assert (and (some? query)
               (or (not (coll? query))
                   (seq query)))
          (format "Compiled query should not be nil/empty. Got: %s" (pr-str query)))
  (let [query (vary-meta query (fn [metta]
                                 (merge (dissoc (meta *built-query*) :type) metta)))]
    (transduce-execute rf query-type model query)))

;;; TODO -- this is a little JDBC-specific. What if some other query engine wants to run plain string queries without us
;;; wrapping them in a vector? Maybe this is something that should be handled at the query execution level in
;;; [[transduce-execute-with-connection]] instead. I guess that wouldn't actually work because we need to attach
;;; metadata to compiled queries
(m/defmethod transduce-compile [#_query-type :default #_model :default #_built-query String]
  [rf query-type model sql]
  (transduce-compile rf query-type model [sql]))

(m/defmethod transduce-compile [#_query-type :default #_model :default #_built-query clojure.lang.IPersistentMap]
  [rf query-type model m]
  (transduce-compile rf query-type model (vary-meta m assoc :type (map/backend))))

;;;; [[transduce-build]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-build
  "The fifth step in the query execution pipeline. Called with a resolved query immediately before 'building' it.

  ```
  transduce-resolve
  ↓
  (resolve query)
  ↓
  transduce-build ← YOU ARE HERE
  ↓
  (build query)
  ↓
  transduce-compile
  ```

  The default implementation builds the resolved query and then calls [[transduce-compile]]."
  {:arglists '([rf query-type₁ model₂ parsed-args resolved-query₃])}
  (dispatch-ignore-rf
   (fn [query-type₁ model₂ _parsed-args resolved-query₃]
     (u/dispatch-on-first-three-args query-type₁ model₂ resolved-query₃))))

(m/defmethod transduce-build :around :default
  [rf query-type model parsed-args resolved-query]
  {:pre [(ifn? rf) (query-type? query-type) (map? parsed-args)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-build
              (m/dispatch-value transduce-build rf query-type model parsed-args resolved-query))
  (log/debugf :compile "Build %s" resolved-query)
  (assert (not (keyword? resolved-query))
          (format "This doesn't look like a resolved query: %s" resolved-query))
  (u/try-with-error-context ["with resolved query" {:query-type     query-type
                                                    :resolved-query resolved-query
                                                    :parsed-args    parsed-args}]
    (next-method rf query-type model parsed-args resolved-query)))

(m/defmethod transduce-build :default
  [rf query-type model parsed-args resolved-query]
  ;; keep the original info around as metadata in case someone needs it later.
  (let [built-query (vary-meta resolved-query
                               assoc
                               ;; HACK in case you need it later. (We do.) See if there's a way we could do this
                               ;; without a big ol ugly HACK.
                               ::parsed-args parsed-args)]
    (transduce-compile rf query-type model built-query)))

;;; Something like (select my-model nil) should basically mean SELECT * FROM my_model WHERE id IS NULL
(m/defmethod transduce-build [#_query-type :default #_model :default #_query nil]
  [rf query-type model parsed-args _nil]
  ;; if `:query` is present but equal to `nil`, treat that as if the pk value IS NULL
  (let [parsed-args (assoc-in parsed-args [:kv-args :toucan/pk] nil)]
    (transduce-build rf query-type model parsed-args {})))

;;; Treat lone integers as queries to select an integer primary key

(m/defmethod transduce-build [#_query-type :default #_model :default #_query Integer]
  [rf query-type model parsed-args n]
  (transduce-build rf query-type model parsed-args (long n)))

(m/defmethod transduce-build [#_query-type :default #_model :default #_query Long]
  [rf query-type model parsed-args pk]
  (transduce-build rf query-type model (update parsed-args :kv-args assoc :toucan/pk pk) {}))

;;; default implementation for maps

(m/defmethod transduce-build [#_query-type :default #_model :default #_query :toucan.map-backend/*]
  [rf query-type model {:keys [kv-args], :as parsed-args} m]
  (let [m (query/apply-kv-args model m kv-args)]
    (next-method rf query-type model (dissoc parsed-args :kv-args) m)))

(m/defmethod transduce-build [#_query-type :default #_model :default #_query clojure.lang.IPersistentMap]
  [rf query-type model parsed-args m]
  (transduce-build rf query-type model parsed-args (vary-meta m assoc :type (map/backend))))

;;; handle SQL or [SQL & args]

(m/defmethod transduce-build [#_query-type :default
                              #_model      :default
                              #_query      String]
  [rf query-type model parsed-args sql]
  (transduce-build rf query-type model parsed-args [sql]))

(m/defmethod transduce-build [#_query-type :default
                              #_model      :default
                              #_query      clojure.lang.Sequential]
  [rf query-type model {:keys [kv-args], :as parsed-args} sql-args]
  ;; TODO -- this isn't necessarily a sql-args query. But we don't currently support this either way.
  (when (seq kv-args)
    (throw (ex-info "key-value args are not supported for plain SQL queries."
                    {:query-type     query-type
                     :model          model
                     :parsed-args    parsed-args
                     :method         #'transduce-build
                     :dispatch-value (m/dispatch-value transduce-build rf query-type model parsed-args sql-args)})))
  (next-method rf query-type model parsed-args sql-args))

;;;; [[transduce-resolve]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-resolve
  "The fourth step in the query execution pipeline.

  ```
  transduce-with-model
  ↓
  transduce-resolve ← YOU ARE HERE
  ↓
  (resolve query)
  ↓
  transduce-build
  ```

  The default implementation does nothing special with `unresolved-query`, and passes it directly
  to [[transduce-build]]."
  {:arglists '([rf query-type₁ model₂ parsed-args unresolved-query₃])}
  (dispatch-ignore-rf (fn [query-type model _parsed-args unresolved-query]
                        (u/dispatch-on-first-three-args query-type model unresolved-query))))

(m/defmethod transduce-resolve :around :default
  [rf query-type model parsed-args unresolved-query]
  {:pre [(ifn? rf) (query-type? query-type) (map? parsed-args)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-resolve
              (m/dispatch-value transduce-resolve rf query-type model parsed-args unresolved-query))
  (log/debugf :compile "Resolve %s" unresolved-query)
  (u/try-with-error-context ["with unresolved query" {:query-type query-type
                                                      :unresolved-query unresolved-query
                                                      :parsed-args      parsed-args}]
    (next-method rf query-type model parsed-args unresolved-query)))

(m/defmethod transduce-resolve :default
  [rf query-type model parsed-args unresolved-query]
  (transduce-build rf query-type model parsed-args unresolved-query))

;;;; [[transduce-with-model]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-with-model
  "The third step in the query execution pipeline. This is the first step that dispatches off of resolved model.

  ```
  transduce-parsed
  ↓
  toucan2.model/resolve-model
  ↓
  transduce-with-model ← YOU ARE HERE
  ↓
  transduce-resolve
  ```

  The default implementation does nothing special and calls [[transduce-resolve]] with the `:queryable` in
  `parsed-args`."
  {:arglists '([rf query-type₁ model₂ parsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-two-args))

(m/defmethod transduce-with-model :around :default
  [rf query-type model parsed-args]
  {:pre [(ifn? rf) (query-type? query-type) (map? parsed-args)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-with-model
              (m/dispatch-value transduce-with-model rf query-type model parsed-args))
  (u/try-with-error-context ["with model" {:model model}]
    (next-method rf query-type model parsed-args)))

(m/defmethod transduce-with-model :default
  [rf query-type model {:keys [queryable], :as parsed-args}]
  ;; if we literally parsed the queryable `nil` then leave it as is; if there is no `:queryable` at all default to a
  ;; map.
  (let [queryable (if (contains? parsed-args :queryable)
                    queryable
                    (or queryable {}))]
    (transduce-resolve rf query-type model (dissoc parsed-args :queryable) queryable)))

;;;; [[transduce-parsed]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-parsed
  "The second step in the query execution pipeline. Called with args as parsed by something like
  [[toucan2.query/parse-args]].

  ```
  transduce-unparsed
  ↓
  (parse args)
  ↓
  transduce-parsed ← YOU ARE HERE
  ↓
  toucan2.model/resolve-model
  ↓
  transduce-with-model
  ```

  The default implementation resolves the `:modelable` in `parsed-args` with [[toucan2.model/resolve-model]] and then
  calls [[transduce-with-model]]."
  {:arglists '([rf query-type₁ parsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

(m/defmethod transduce-parsed :around :default
  [rf query-type parsed-args]
  {:pre [(ifn? rf) (query-type? query-type) (map? parsed-args)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-parsed
              (m/dispatch-value transduce-parsed rf query-type parsed-args))
  (u/try-with-error-context ["with parsed args" {:parsed-args parsed-args}]
    (next-method rf query-type parsed-args)))

(m/defmethod transduce-parsed :default
  [rf query-type {:keys [modelable connectable], :as parsed-args}]
  (let [model (model/resolve-model modelable)
        thunk (^:once fn* []
               (transduce-with-model rf query-type model (dissoc parsed-args :modelable :connectable)))]
    (if connectable
      (binding [conn/*current-connectable* connectable]
        (thunk))
      (thunk))))

;;;; [[transduce-unparsed]]

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} transduce-unparsed
  "The first step in the query execution pipeline. Called with the unparsed args as passed to something
  like [[toucan2.select/select]], before parsing the args.

  ```
  Entrypoint e.g. select/select
  ↓
  transduce-unparsed           ← YOU ARE HERE
  ↓
  (parse args)
  ↓
  transduce-parsed
  ```

  The default implementation parses the args with [[toucan2.query/parse-args]] and then
  calls [[transduce-parsed]]."
  {:arglists '([rf query-type₁ unparsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

(m/defmethod transduce-unparsed :around :default
  [rf query-type unparsed]
  {:pre [(ifn? rf) (query-type? query-type) (sequential? unparsed)]}
  (log/tracef :compile
              "In %s with dispatch value %s"
              `transduce-unparsed
              (m/dispatch-value transduce-unparsed rf query-type unparsed))
  (u/try-with-error-context ["transduce results" {:query-type query-type, :unparsed unparsed}]
    (next-method rf query-type unparsed)))

(m/defmethod transduce-unparsed :default
  [rf query-type unparsed]
  (let [parsed-args (query/parse-args query-type unparsed)]
    (transduce-parsed rf query-type parsed-args)))

;;;; rf helper functions

(defn with-init
  "Returns a version of reducing function `rf` with a zero-arity (initial value arity) that returns `init`."
  [rf init]
  (fn
    ([]    init)
    ([x]   (rf x))
    ([x y] (rf x y))))

(m/defmulti ^:dynamic #_{:clj-kondo/ignore [:dynamic-var-not-earmuffed]} default-rf
  {:arglists '([query-type])}
  keyword)

(m/defmethod default-rf :toucan.result-type/update-count
  [_query-type]
  (-> (fnil + 0 0)
      (with-init 0)
      completing))

(m/defmethod default-rf :toucan.result-type/*
  [_query-type]
  ((map realize/realize) conj))

;;; This doesn't work for things that return update counts!
(defn first-result-rf [rf]
  (completing ((take 1) rf) first))

;;;; Helper functions for implementing stuff like [[toucan2.select/select]]

(defn transduce-unparsed-with-default-rf [query-type unparsed]
  (assert (query-type? query-type))
  (let [rf (default-rf query-type)]
    (transduce-unparsed rf query-type unparsed)))

(defn transduce-unparsed-first-result [query-type unparsed]
  (let [rf (default-rf query-type)]
    (transduce-unparsed (first-result-rf rf) query-type unparsed)))


;;;; reducible versions for implementing stuff like [[toucan2.select/reducible-select]]

(defn- reducible-fn
  "Create a reducible with one of the functions in this namespace."
  [f & args]
  (reify
    clojure.lang.IReduceInit
    (reduce [_this rf init]
      ;; wrap the rf in `completing` so we don't end up doing any special one-arity TRANSDUCE stuff inside of REDUCE
      (apply f (completing (with-init rf init)) args))

    pretty/PrettyPrintable
    (pretty [_this]
      (list* `reducible-fn f args))))

(defn reducible-unparsed
  [query-type unparsed]
  (reducible-fn transduce-unparsed query-type unparsed))

(defn reducible-parsed-args
  [query-type parsed-args]
  (reducible-fn transduce-parsed query-type parsed-args))

(defn reducible-with-model
  [query-type model parsed-args]
  (reducible-fn transduce-with-model query-type model parsed-args))

;;; These aren't used and you can easily make them yourself if you need them so they are commented out for now. If we
;;; actually need them we can uncomment them.

;; (defn reducible-resolved-query
;;   [query-type model parsed-args resolved-query]
;;   (reducible-fn transduce-build query-type model parsed-args resolved-query))

;; (defn reducible-built-query
;;   [query-type model built-query]
;;   (reducible-fn transduce-compile query-type model built-query))

;; (defn reducible-compiled-query
;;   [query-type model compiled-query]
;;   (reducible-fn transduce-execute query-type model compiled-query))

;; (defn reducible-compiled-query-with-connection
;;   [conn query-type model compiled-query]
;;   (reducible-fn transduce-execute-with-connection conn query-type model compiled-query))

;;;; utils

(defn parent-query-type [query-type]
  (some (fn [k]
          (when (isa? k :toucan.query-type/*)
            k))
        (parents query-type)))

(defn base-query-type
  "E.g. something like `:toucan.query-type/insert.*`. The immediate descendant of `:toucan.query-type/*`.

  ```clj
  (base-query-type :toucan.query-type/insert.instances)
  =>
  :toucan.query-type/insert.*
  ```"
  [query-type]
  (when (isa? query-type :toucan.query-type/*)
    (loop [last-query-type nil, query-type query-type]
      (if (or (= query-type :toucan.query-type/*)
              (not query-type))
        last-query-type
        (recur query-type (parent-query-type query-type))))))

(defn similar-query-type-returning
  "```clj
  (similar-query-type-returning :toucan.query-type/insert.instances :toucan.result-type/pks)
  =>
  :toucan.query-type/insert.pks
  ```"
  [query-type result-type]
  (let [base-type (base-query-type query-type)]
    (some (fn [descendant]
            (when (and ((parents descendant) base-type)
                       (isa? descendant result-type))
              descendant))
          (descendants base-type))))

;;;; default JDBC impls. Not convinced they belong here.

(m/defmethod transduce-execute-with-connection [#_connection java.sql.Connection
                                                #_query-type :default
                                                #_model      :default]
  [rf conn query-type model sql-args]
  (increment-call-count!)
  ;; `:return-keys` is passed in this way instead of binding a dynamic var because we don't want any additional queries
  ;; happening inside of the `rf` to return keys or whatever.
  (let [extra-options (when (isa? query-type :toucan.result-type/pks)
                        {:return-keys true})
        result        (jdbc.query/reduce-jdbc-query rf (rf) conn model sql-args extra-options)]
    (rf result)))

;;; To get Databases to return the generated primary keys rather than the update count for SELECT/UPDATE/DELETE we need
;;; to set the `next.jdbc` option `:return-keys true`

(m/defmethod transduce-execute-with-connection [#_connection java.sql.Connection
                                                #_query-type :toucan.result-type/pks
                                                #_model      :default]
  [rf conn query-type model sql-args]
  (let [rf* ((map (model/select-pks-fn model))
             rf)]
    (binding [jdbc.query/*options* (assoc jdbc.query/*options* :return-keys true)]
      (next-method rf* conn query-type model sql-args))))

;;; There's no such thing as "returning instances" for INSERT/DELETE/UPDATE. So we will fake it by executing the query
;;; with the equivalent `returning-pks` query type, and then do a `:select` query to get the matching instances and pass
;;; those in to the original `rf`.

;;; this is here in case we need to differentiate it from a regular select for some reason or another
(derive ::select.instances-from-pks :toucan.query-type/select.instances)

(defn- transduce-instances-from-pks
  [rf model columns pks]
  ;; make sure [[toucan2.select]] is loaded so we get the impls for `:toucan.query-type/select.instances`
  (when-not (contains? (loaded-libs) 'toucan2.select)
    (locking clojure.lang.RT/REQUIRE_LOCK
      (require 'toucan2.select)))
  (if (empty? pks)
    []
    (let [kv-args     {:toucan/pk [:in pks]}
          parsed-args {:columns   columns
                       :kv-args   kv-args
                       :queryable {}}]
      (transduce-with-model rf ::select.instances-from-pks model parsed-args))))

(m/defmethod transduce-execute-with-connection [#_connection java.sql.Connection
                                                #_query-type :toucan.result-type/instances
                                                #_model      :default]
  [rf conn query-type model sql-args]
  ;; for non-DML stuff (ie `SELECT`) JDBC can actually return instances with zero special magic, so we can just let the
  ;; next method do it's thing. Presumably if we end up here with something that is neither DML or DQL, but maybe
  ;; something like a DDL `CREATE TABLE` statement, we probably don't want to assume it has the possibility to have it
  ;; return generated PKs.
  (let [DML?          (isa? query-type :toucan.statement-type/DML)
        pk-query-type (when DML?
                        (similar-query-type-returning query-type :toucan.result-type/pks))]
    (if-not pk-query-type
      ;; non-DML query or if we don't know how to do magic, let the `next-method` do it's thing.
      (next-method rf conn query-type model sql-args)
      ;; for DML stuff get generated PKs and then do a SELECT to get the rows. See if we know how the right
      ;; returning-PKs query type.
      ;;
      ;; We're using `conj` here instead of `rf` so no row-transform nonsense or whatever is done. We will pass the
      ;; actual instances to the original `rf` once we get them.
      ;;
      ;;
      (let [pks     (transduce-execute-with-connection conj conn pk-query-type model sql-args)
            ;; this is sort of a hack but I don't know of any other way to pass along `:columns` information with the
            ;; original parsed args
            columns (get-in (meta sql-args) [::parsed-args :columns])]
        ;; once we have a sequence of PKs then get instances as with `select` and do our magic on them using the
        ;; ORIGINAL `rf`.
        (transduce-instances-from-pks rf model columns pks)))))

;;;; Misc util functions

(defn resolve-query
  "Helper for getting the resolved query for `unresolved-query`."
  [query-type model unresolved-query]
  (binding [transduce-build (fn [_rf _query-type _model₂ _parsed-args resolved-query]
                              resolved-query)]
    (transduce-resolve conj query-type model {} unresolved-query)))

(defn build
  "Helper for getting a built query for a `resolved-query`."
  [query-type model parsed-args resolved-query]
  (binding [transduce-compile (fn [_rf _query-type _model query]
                                    query)]
    (transduce-with-model conj query-type model (assoc parsed-args :queryable resolved-query))))

(defn compile
  "Helper for compiling a `built-query` to something that can be executed natively."
  ([built-query]
   (compile nil built-query))
  ([query-type built-query]
   (compile query-type nil built-query))
  ([query-type model built-query]
   (binding [transduce-execute (fn [_rf _query-type _model query]
                                        query)]
     (transduce-compile conj (or query-type :toucan.query-type/*) model built-query))))

(defn do-traced-pipeline
  "E.g.

  ```clj
  (do-traced-pipeline #{:with-model} (fn [] ...))
  ```"
  ([thunk]
   (do-traced-pipeline (constantly true) thunk))
  ([topics thunk]
   (letfn [(traced-var [varr]
             (partial m.trace/trace* (vary-meta (var-get varr)
                                                assoc
                                                ::m.trace/description (symbol varr))))]
     (with-bindings* (merge
                      (when (topics :unparsed)
                        {#'transduce-unparsed (traced-var #'transduce-unparsed)})
                      (when (topics :parsed)
                        {#'transduce-parsed (traced-var #'transduce-parsed)})
                      (when (topics :with-model)
                        {#'transduce-with-model (traced-var #'transduce-with-model)})
                      (when (topics :resolve)
                        {#'transduce-resolve (traced-var #'transduce-resolve)})
                      (when (topics :build)
                        {#'transduce-build (traced-var #'transduce-build)})
                      (when (topics :compile)
                        {#'transduce-compile (traced-var #'transduce-compile)})
                      (when (topics :with-query)
                        {#'transduce-execute (traced-var #'transduce-execute)})
                      (when (topics :execute)
                        {#'transduce-execute-with-connection (traced-var #'transduce-execute-with-connection)}))
                     thunk))))
