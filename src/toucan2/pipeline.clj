(ns toucan2.pipeline
  "This is a low-level namespace implementing our query execution pipeline. Most of the stuff you'd use on a regular basis
  are implemented on top of stuff here."
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.jdbc.query :as jdbc.query]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.util :as u]
   [methodical.util.trace :as m.trace]))

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

;;;; pipeline

;;; The little subscripts below are so you can tell at a glance which args are used for dispatch.

(defn- dispatch-ignore-rf [f]
  (fn [_rf & args]
    (apply f args)))

(m/defmulti transduce-unparsed*
  "The first step in the query execution pipeline. Called with the unparsed args as passed to something
  like [[toucan2.select/select]], before parsing the args.

  ```
  Entrypoint e.g. select/select
  ↓
  transduce-unparsed*           ← YOU ARE HERE
  ↓
  toucan2.query/parse-args
  ↓
  transduce-parsed-args*
  ```

  The default implementation parses the args with [[toucan2.query/parse-args]] and then
  calls [[transduce-parsed-args*]]."
  {:arglists '([rf query-type₁ unparsed])}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

(m/defmulti transduce-parsed-args*
  "The second step in the query execution pipeline. Called with args as parsed by [[toucan2.query/parse-args]].

  ```
  transduce-unparsed*
  ↓
  toucan2.query/parse-args
  ↓
  transduce-parsed-args* ← YOU ARE HERE
  ↓
  toucan2.model/with-model
  ↓
  transduce-with-model*
  ```

  The default implementation resolves the `:modelable` in `parsed-args` with [[toucan2.model/with-model]] and then
  calls [[transduce-with-model*]]."
  {:arglists '([rf query-type₁ parsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

(m/defmulti transduce-with-model*
  "The third step in the query execution pipeline. This is the first step that dispatches off of resolved model.

  ```
  transduce-parsed-args*
  ↓
  toucan2.model/with-model
  ↓
  transduce-with-model* ← YOU ARE HERE
  ↓
  toucan2.query/with-resolved-query
  ↓
  transduce-resolved-query*
  ```

  The default implementation resolves the queryable with [[toucan2.query/with-resolved-query]] and then
  calls [[transduce-resolved-query*]]."
  {:arglists '([rf query-type₁ model₂ parsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-two-args))

(m/defmulti transduce-resolved-query*
  "The fourth step in the query execution pipeline. Called with a resolved query immediately before 'building' it.

  ```
  transduce-with-model*
  ↓
  toucan2.query/with-resolved-query
  ↓
  transduce-resolved-query* ← YOU ARE HERE
  ↓
  toucan2.query/with-built-query
  ↓
  transduce-built-query*
  ```

  The default implementation builds the resolved query with [[toucan2.query/with-built-query]] and then
  calls [[transduce-built-query*]]."
  {:arglists '([rf query-type₁ model₂ parsed-args resolved-query₃])}
  (dispatch-ignore-rf
   (fn [query-type₁ model₂ _parsed-args resolved-query₃]
     (u/dispatch-on-first-three-args query-type₁ model₂ resolved-query₃))))

(m/defmulti transduce-built-query*
  "The fifth step in the query execution pipeline. Called with a query that is ready to be compiled, e.g. a fully-formed
  Honey SQL form.

  ```
  transduce-resolved-query*
  ↓
  toucan2.query/with-built-query
  ↓
  transduce-built-query* ← YOU ARE HERE
  ↓
  toucan2.compile/with-compiled-query
  ↓
  transduce-compiled-query*
  ```

  The default implementation compiles the query with [[toucan2.compile/with-compiled-query]] and then
  calls [[transduce-compiled-query*]]."
  {:arglists '([rf query-type₁ model₂ built-query₃])}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

(m/defmulti transduce-compiled-query*
  "The sixth step in the query execution pipeline. Called with a compiled query that is ready to be executed natively, for
  example a `[sql & args]` vector, immediately before opening a connection.

  ```
  transduce-built-query*
  ↓
  toucan2.compile/with-compiled-query
  ↓
  transduce-compiled-query* ← YOU ARE HERE
  ↓
  toucan2.connection/with-connection
  ↓
  transduce-compiled-query-with-connection*
  ```

  The default implementation opens a connection (or uses the current connection)
  using [[toucan2.connection/with-connection]] and then calls [[transduce-compiled-query-with-connection*]]."
  {:arglists '([rf query-type₁ model₂ compiled-query₃])}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

(m/defmulti transduce-compiled-query-with-connection*
  "The seventh and final step in the query execution pipeline. Called with a fully compiled query that can be executed
  natively, and an open connection for executing it.

  ```
  transduce-compiled-query*
  ↓
  toucan2.connection/with-connection
  ↓
  transduce-compiled-query-with-connection* ← YOU ARE HERE
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

;;;; fn versions.

;;; TODO -- how much are these really used or needed? Can we just make all the multimethods here dynamic instead?

(def ^:dynamic ^{:arglists '([rf query-type unparsed])}
  *transduce-unparsed*
  #'transduce-unparsed*)

(def ^:dynamic ^{:arglists '([rf query-type parsed-args])}
  *transduce-parsed-args*
  #'transduce-parsed-args*)

(def ^:dynamic ^{:arglists '([rf query-type model parsed-args])}
  *transduce-with-model*
  #'transduce-with-model*)

(def ^:dynamic ^{:arglists '([rf query-type model parsed-args resolved-query])}
  *transduce-resolved-query*
  #'transduce-resolved-query*)

(def ^:dynamic ^{:arglists '([rf query-type model built-query])}
  *transduce-built-query*
  #'transduce-built-query*)

(def ^:dynamic ^{:arglists '([rf query-type model compiled-query])}
  *transduce-compiled-query*
  #'transduce-compiled-query*)

(def ^:dynamic ^{:arglists '([rf conn query-type model compiled-query])}
  *transduce-compiled-query-with-connection*
  #'transduce-compiled-query-with-connection*)

;;;; rf helper functions

(defn with-init [rf init]
  (fn
    ([]    init)
    ([x]   (rf x))
    ([x y] (rf x y))))

(m/defmulti default-rf
  {:arglists '([query-type])}
  keyword)

(m/defmethod default-rf :toucan.result-type/update-count
  [_query-type]
  (-> (fnil + 0 0)
      (with-init 0)
      completing))

(m/defmethod default-rf :toucan.result-type/*
  [_query-type]
  conj)

;;; This doesn't work for things that return update counts!
(defn first-result-rf [rf]
  (completing ((take 1) rf) first))

;;;; function wrappers for the underlying methods.

(defn transduce-unparsed
  ([query-type unparsed]
   (assert (isa? query-type :toucan.result-type/*))
   (let [rf (default-rf query-type)]
     (transduce-unparsed rf query-type unparsed)))

  ([rf query-type unparsed]
   {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (sequential? unparsed)]}
   (u/println-debug ["transduce unparsed %s args %s" query-type unparsed])
   (u/try-with-error-context ["transduce results" {:query-type query-type, :unparsed unparsed}]
     (*transduce-unparsed* rf query-type unparsed))))

(defn transduce-unparsed-first-result [query-type unparsed]
  (let [rf (default-rf query-type)]
    (transduce-unparsed (first-result-rf rf) query-type unparsed)))

(defn transduce-parsed-args
  [rf query-type parsed-args]
  {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (map? parsed-args)]}
  (u/println-debug ["transduce parsed args %s" parsed-args])
  (u/try-with-error-context ["with parsed args" {:parsed-args parsed-args}]
    (*transduce-parsed-args* rf query-type parsed-args)))

(defn transduce-with-model
  [rf query-type model parsed-args]
  {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (map? parsed-args)]}
  (u/println-debug ["transduce for model %s" model])
  (u/try-with-error-context ["with model" {:model model}]
    (*transduce-with-model* rf query-type model parsed-args)))

(defn transduce-resolved-query
  [rf query-type model parsed-args resolved-query]
  {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (map? parsed-args)]}
  (u/println-debug ["transduce resolved query %s" resolved-query])
  (u/try-with-error-context ["with resolved query" {:query-type     query-type
                                                    :resolved-query resolved-query
                                                    :parsed-args    parsed-args}]
    (*transduce-resolved-query* rf query-type model parsed-args resolved-query)))

(defn transduce-built-query
  [rf query-type model built-query]
  {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (some? built-query)]}
  (u/println-debug ["transduce built query %s" built-query])
  (u/try-with-error-context ["with built query" {:query-type query-type, :built-query built-query}]
    (*transduce-built-query* rf query-type model built-query)))

(defn transduce-compiled-query
  [rf query-type model compiled-query]
  {:pre [(ifn? rf) (isa? query-type :toucan.result-type/*) (some? compiled-query)]}
  (u/println-debug ["transduce compiled query %s" compiled-query])
  (u/try-with-error-context ["with compiled query" {:compiled-query compiled-query}]
    (*transduce-compiled-query* rf query-type model compiled-query)))

(defn transduce-compiled-query-with-connection
  [rf conn query-type model compiled-query]
  {:pre [(ifn? rf) (some? conn) (isa? query-type :toucan.result-type/*) (some? compiled-query)]}
  (u/println-debug ["transduce query with %s connection" (symbol (.getCanonicalName (class conn)))])
  (u/try-with-error-context ["with connection" {:connection (symbol (.getCanonicalName (class conn)))}]
    (*transduce-compiled-query-with-connection* rf conn query-type model compiled-query)))

;;;; default impls

(def ^:dynamic *call-count-thunk*
  "Thunk function to call every time a query is executed if [[toucan2.execute/with-call-count]] is in use. Implementees
  of [[transduce-compiled-query-with-connection*]] should invoke this every time a query gets executed. You can
  use [[increment-call-count!]] to simplify the chore of making sure it's non-`nil` before invoking it."
  nil)

(defn increment-call-count! []
  (when *call-count-thunk* (*call-count-thunk*)))

(m/defmethod transduce-unparsed* :default
  [rf query-type unparsed]
  (let [parsed-args (query/parse-args query-type unparsed)]
    (transduce-parsed-args rf query-type parsed-args)))

(m/defmethod transduce-parsed-args* :default
  [rf query-type {:keys [modelable], :as parsed-args}]
  (model/with-model [model modelable]
    (transduce-with-model rf query-type model (dissoc parsed-args :modelable))))

(m/defmethod transduce-with-model* :default
  [rf query-type model {:keys [queryable], :as parsed-args}]
  (query/with-resolved-query [resolved-query [model queryable]]
    ;; disabled for now since it breaks [[toucan2.no-jdbc-poc-test]]... but we should figure out how to fix that and
    ;; then reenable this.
    #_(assert (not (keyword? resolved-query))
              (str (format "This doesn't seem like a RESOLVED query: %s" resolved-query)
                   \newline
                   (format "Do you need to implement %s for %s?"
                           `query/do-with-resolved-query
                           (m/dispatch-value query/do-with-resolved-query model resolved-query))))
    (transduce-resolved-query rf query-type model (dissoc parsed-args :queryable) resolved-query)))

(m/defmethod transduce-resolved-query* :default
  [rf query-type model parsed-args resolved-query]
  (query/with-built-query [built-query [query-type model parsed-args resolved-query]]
    ;; keep the original info around as metadata in case someone needs it later.
    ;;
    ;; TODO -- consider whether the [[with-built-query]] macro should do this for us.
    (let [built-query (vary-meta built-query
                                 assoc
                                 ;; HACK in case you need it later. (We do.) See if there's a way we could do this
                                 ;; without a big ol ugly HACK.
                                 ::resolved-query resolved-query
                                 ::parsed-args parsed-args)]
      (transduce-built-query rf query-type model built-query))))

(m/defmethod transduce-built-query* :default
  [rf query-type model built-query]
  (compile/with-compiled-query [compiled-query [model built-query]]
    ;; keep the original info around as metadata in case someone needs it later.
    (let [compiled-query (vary-meta compiled-query #(merge (meta built-query)
                                                           {::built-query built-query}
                                                           %))]
      (transduce-compiled-query rf query-type model compiled-query))))

(defn- current-connectable [model]
  (or conn/*current-connectable*
      (model/default-connectable model)))

(m/defmethod transduce-compiled-query* :default
  [rf query-type model compiled-query]
  (conn/with-connection [conn (current-connectable model)]
    (transduce-compiled-query-with-connection rf conn query-type model compiled-query)))

;;; For DML stuff we will run the whole thing in a transaction if we're not already in one.
;;;
;;; Not 100% sure this is necessary since we would probably already be in one if we needed to be because stuff like
;;; [[toucan2.tools.before-delete]] have to put us in one much earlier.
(m/defmethod transduce-compiled-query* [#_query-type     :toucan.statement-type/DML
                                        #_model          :default
                                        #_compiled-query :default]
  [rf query-type model compiled-query]
  (conn/with-transaction [_conn (current-connectable model) {:nested-transaction-rule :ignore}]
    (next-method rf query-type model compiled-query)))

;;;; reducible versions

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
  (reducible-fn transduce-parsed-args query-type parsed-args))

(defn reducible-with-model
  [query-type model parsed-args]
  (reducible-fn transduce-with-model query-type model parsed-args))

(defn reducible-resolved-query
  [query-type model parsed-args resolved-query]
  (reducible-fn transduce-resolved-query query-type model parsed-args resolved-query))

(defn reducible-built-query
  [query-type model built-query]
  (reducible-fn transduce-built-query query-type model built-query))

(defn reducible-compiled-query
  [query-type model compiled-query]
  (reducible-fn transduce-compiled-query query-type model compiled-query))

(defn reducible-compiled-query-with-connection
  [conn query-type model compiled-query]
  (reducible-fn transduce-compiled-query-with-connection conn query-type model compiled-query))

;;;; proof-of-concept: simple out transform

;;; TODO -- move this to `tools`.

(defmacro define-out-transform
  {:style/indent :defn}
  [[query-type model-type] [instance-binding] & body]
  `(m/defmethod transduce-with-model* :around [~query-type ~model-type]
     [rf# ~'&query-type ~'&model ~'&parsed-args]
     (let [rf*# ((map (fn [~instance-binding]
                        (let [instance# (do ~@body)]
                          (cond-> instance#
                            (instance/instance? instance#)
                            instance/reset-original))))
                 rf#)]
       (~'next-method rf*# ~'&query-type ~'&model ~'&parsed-args))))

(s/fdef define-out-transform
  :args (s/cat :dispatch-value (s/spec (s/cat :query-type keyword?
                                              :model-type any?))
               :bindings (s/spec (s/cat :row :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)

(comment
  (define-out-transform [:toucan.query-type/select.instances ::venues]
    [row]
    (assoc row :neat true)))

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

(m/defmethod transduce-compiled-query-with-connection* [#_connection java.sql.Connection
                                                        #_query-type :default
                                                        #_model      :default]
  [rf conn query-type model sql-args]
  (increment-call-count!)
  ;; `:return-keys` is passed in this way instead of binding a dynamic var because we don't want any additional queries
  ;; happening inside of the `rf` to return keys or whatever.
  (let [extra-options (when (isa? query-type :toucan.result-type/pks)
                        {:return-keys true})
        result        (jdbc.query/reduce-jdbc-query conn model sql-args rf (rf) extra-options)]
    (rf result)))

;;; To get Databases to return the generated primary keys rather than the update count for SELECT/UPDATE/DELETE we need
;;; to set the `next.jdbc` option `:return-keys true`

(m/defmethod transduce-compiled-query-with-connection* [#_connection java.sql.Connection
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

;; (s/def ::pk.raw-value (complement coll?))

;; (s/def ::pk  (s/alt :raw-value ::pk.raw-value
;;                     :compound  (s/coll-of ::pk.raw-value)))
;; (s/def ::pks (s/nilable (s/coll-of ::pk)))

(defn- transduce-instances-from-pks
  [rf model columns pks]
  ;; make sure [[toucan2.select]] is loaded so we get the impls for `:toucan.query-type/select.instances`
  (locking clojure.lang.RT/REQUIRE_LOCK
    (require 'toucan2.select))
  (if (empty? pks)
    []
    (let [kv-args     {:toucan/pk [:in pks]}
          parsed-args {:columns   columns
                       :kv-args   kv-args
                       :queryable {}}]
      (transduce-with-model rf ::select.instances-from-pks model parsed-args))))

(m/defmethod transduce-compiled-query-with-connection* [#_connection java.sql.Connection
                                                        #_query-type :toucan.result-type/instances
                                                        #_model      :default]
  [rf conn query-type model sql-args]
  ;; for non-DML stuff (ie `SELECT`) JDBC can actually return instances with zero special magic, so we can just let the
  ;; next method do it's thing. Presumably if we end up here with something that is neither DML or DQL, but maybe
  ;; something like a DDL `CREATE TABLE` statement, we probably don't want to assume it has the possibility to have it
  ;; return generated PKs.
  (let [dml?          (isa? query-type :toucan.statement-type/DML)
        pk-query-type (when dml?
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
      (let [pks     (transduce-compiled-query-with-connection* conj conn pk-query-type model sql-args)
            ;; this is sort of a hack but I don't know of any other way to pass along `:columns` information with the
            ;; original parsed args
            columns (get-in (meta sql-args) [::parsed-args :columns])]
        ;; once we have a sequence of PKs then get instances as with `select` and do our magic on them using the
        ;; ORIGINAL `rf`.
        (transduce-instances-from-pks rf model columns pks)))))

;;;; Tracing

(defn do-traced-pipeline [thunk]
  (letfn [(traced-var [varr]
            (partial m.trace/trace* (vary-meta (var-get varr)
                                               assoc
                                               ::m.trace/description (symbol varr))))]
    (binding [*transduce-unparsed*                       (traced-var #'transduce-unparsed*)
              *transduce-parsed-args*                    (traced-var #'transduce-parsed-args*)
              *transduce-with-model*                     (traced-var #'transduce-with-model*)
              *transduce-resolved-query*                 (traced-var #'transduce-resolved-query*)
              *transduce-built-query*                    (traced-var #'transduce-built-query*)
              *transduce-compiled-query*                 (traced-var #'transduce-compiled-query*)
              *transduce-compiled-query-with-connection* (traced-var #'transduce-compiled-query-with-connection*)]
      (thunk))))
