(ns toucan2.pipeline
  "This is a low-level namespace implementing our query execution pipeline. Most of the stuff you'd use on a regular basis
  are implemented on top of stuff here.

  Pipeline order is

  1. [[toucan2.query/parse-args]]    (entrypoint fn: [[transduce-unparsed]])
  2. [[toucan2.model/resolve-model]] (entrypoint fn: [[transduce-parsed]])
  3. [[resolve]]
  4. [[transduce-query]]
  5. [[build]]
  6. [[compile]]
  7. [[results-transform]]
  8. [[transduce-execute-with-connection]]

  The main pipeline entrypoint is [[transduce-unparsed]]."
  (:refer-clojure :exclude [compile resolve])
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(comment s/keep-me)

;;;; pipeline

(def ^:dynamic ^:no-doc *call-count-thunk*
  "Thunk function to call every time a query is executed if [[toucan2.execute/with-call-count]] is in use. Implementees
  of [[transduce-execute-with-connection]] should invoke this every time a query gets executed. You can
  use [[increment-call-count!]] to simplify the chore of making sure it's non-`nil` before invoking it."
  nil)

;;; TODO -- This name is a little long, maybe this should be called `transduce-execute` and the function should get
;;; called something else
(m/defmulti transduce-execute-with-connection
  "The final stage of the Toucan 2 query execution pipeline. Execute a compiled query (as returned by [[compile]]) with a
  database connection, e.g. a `java.sql.Connection`, and transduce results with reducing function `rf`.

  The only reason you should need to implement this method is if you are writing a new query execution backend."
  {:arglists            '([rf conn₁ query-type₂ model₃ compiled-query])
   :defmethod-arities   #{5}
   :dispatch-value-spec (types/or-default-spec
                         (s/cat :conn       ::types/dispatch-value.keyword-or-class
                                :query-type ::types/dispatch-value.query-type
                                :model      ::types/dispatch-value.model))}
  (fn [_rf conn query-type model _compiled-query]
    (u/dispatch-on-first-three-args conn query-type model)))

(m/defmethod transduce-execute-with-connection :before :default
  "Count all queries that are executed by calling [[*call-count-thunk*]] if bound."
  [_rf _conn _query-type _model query]
  (when *call-count-thunk*
    (*call-count-thunk*))
  query)

(defn- transduce-execute
  "Get a connection from the current connection and call [[transduce-execute-with-connection]]. For DML queries, this
  uses [[conn/with-transaction]] to get a connection and ensure we are in a transaction, if we're not already in one.
  For non-DML queries this uses the usual [[conn/with-connection]]."
  [rf query-type model compiled-query]
  (u/try-with-error-context {::rf rf}
    (if (isa? query-type :toucan.statement-type/DML)
      ;; For DML stuff we will run the whole thing in a transaction if we're not already in one. Not 100% sure this is
      ;; necessary since we would probably already be in one if we needed to be because stuff
      ;; like [[toucan2.tools.before-delete]] have to put us in one much earlier.
      (conn/with-transaction [conn nil {:nested-transaction-rule :ignore}]
        (transduce-execute-with-connection rf conn query-type model compiled-query))
      ;; otherwise we can just execute with a normal non-transaction query.
      (conn/with-connection [conn]
        (transduce-execute-with-connection rf conn query-type model compiled-query)))))

(m/defmulti results-transform
  "The transducer that should be applied to the reducing function executed when running a query of
  `query-type` (see [[toucan2.types]]) for `model` (`nil` if the query is ran without a model, e.g.
  with [[toucan2.execute/query]]). The default implementation returns `identity`; add your own implementations as
  desired to apply additional results transforms.

  Be sure to `comp` the transform from `next-method`:

  ```clj
  (m/defmethod t2/results-transform [:toucan.query-type/select.* :my-model]
    [query-type model]
    (comp (next-method query-type model)
          (map (fn [instance]
                 (assoc instance :num-cans 2)))))
  ```

  It's probably better to put the transducer returned by `next-method` first in the call to `comp`, because `cond` works
  like `->` when composing transducers, and since `next-method` is by definition the less-specific method, it makes
  sense to call that transform before we apply our own. This means our own transforms will get to see the results of the
  previous stage, rather than vice-versa."
  {:arglists            '([query-type₁ model₂])
   :defmethod-arities   #{2}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type-model)}
  u/dispatch-on-first-two-args)

(m/defmethod results-transform :default
  [_query-type _model]
  identity)

(m/defmulti compile
  "Compile a `built-query` to something that can be executed natively by the query execution backend, e.g. compile a Honey
  SQL map to a `[sql & args]` vector.

  You can implement this method to write a custom query compilation backend, for example to compile some certain record
  type in a special way. See [[toucan2.honeysql2]] for an example implementation.

  In addition to dispatching on `query-type` and `model`, this dispatches on the type of `built-query`, in a special
  way: for plain maps this will dispatch on the current [[map/backend]]."
  {:arglists            '([query-type₁ model₂ built-query₃])
   :defmethod-arities   #{3}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type-model-query)}
  u/dispatch-on-first-three-args)

(m/defmethod compile :default
  "Default implementation: return query as-is (i.e., consider it to already be compiled). Check that the query is non-nil
  and, if it is a collection, non-empty. Everything else is fair game."
  [_query-type _model query]
  (assert (and (some? query)
               (or (not (coll? query))
                   (seq query)))
          (format "Compiled query should not be nil/empty. Got: %s" (pr-str query)))
  query)

;;; TODO -- this is a little JDBC-specific. What if some other query engine wants to run plain string queries without us
;;; wrapping them in a vector? Maybe this is something that should be handled at the query execution level in
;;; [[transduce-execute-with-connection]] instead. I guess that wouldn't actually work because we need to attach
;;; metadata to compiled queries
(m/defmethod compile [#_query-type :default #_model :default #_built-query String]
  "Compile a string query. Default impl wraps the string in a vector and recursively calls [[compile]]."
  [query-type model sql]
  (compile query-type model [sql]))

;;; default implementation of [[compile]] for maps lives in [[toucan2.honeysql2]]

;;; TODO -- does this belong here, or in [[toucan2.query]]?
(m/defmulti build
  "Build a query by applying `parsed-args` to `resolved-query` into something that can be compiled by [[compile]], e.g.
  build a Honey SQL query by applying `parsed-args` to an initial `resolved-query` map.

  You can implement this method to write a custom query compilation backend, for example to compile some certain record
  type in a special way. See [[toucan2.honeysql2]] for an example implementation.

  In addition to dispatching on `query-type` and `model`, this dispatches on the type of `resolved-query`, in a special
  way: for plain maps this will dispatch on the current [[map/backend]]."
  {:arglists            '([query-type₁ model₂ parsed-args resolved-query₃])
   :defmethod-arities   #{4}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type-model-query)}
  (fn [query-type model _parsed-args resolved-query]
    (u/dispatch-on-first-three-args query-type model resolved-query)))

(m/defmethod build :default
  [_query-type _model _parsed-args resolved-query]
  resolved-query)

(m/defmethod build [#_query-type :default #_model :default #_resolved-query nil]
  "Something like (select my-model nil) should basically mean SELECT * FROM my_model WHERE id IS NULL"
  [query-type model parsed-args _nil]
  ;; if `:query` is present but equal to `nil`, treat that as if the pk value IS NULL
  (let [parsed-args (assoc-in parsed-args [:kv-args :toucan/pk] nil)]
    (build query-type model parsed-args {})))

(m/defmethod build [#_query-type :default #_model :default #_resolved-query Integer]
  "Treat lone integers as queries to select an integer primary key."
  [query-type model parsed-args n]
  (build query-type model parsed-args (long n)))

(m/defmethod build [#_query-type :default #_model :default #_resolved-query Long]
  "Treat lone integers as queries to select an integer primary key."
  [query-type model parsed-args pk]
  (build query-type model (update parsed-args :kv-args assoc :toucan/pk pk) {}))

(m/defmethod build [#_query-type :default #_model :default #_query String]
  "Default implementation for plain strings. Wrap the string in a vector and recurse."
  [query-type model parsed-args sql]
  (build query-type model parsed-args [sql]))

(m/defmethod build [#_query-type :default #_model :default #_query clojure.lang.Sequential]
  "Default implementation of vector [query & args] queries."
  [query-type model {:keys [kv-args], :as parsed-args} sql-args]
  (when (seq kv-args)
    (throw (ex-info "key-value args are not supported for [query & args]."
                    {:query-type     query-type
                     :model          model
                     :parsed-args    parsed-args
                     :method         #'build
                     :dispatch-value (m/dispatch-value build query-type model parsed-args sql-args)})))
  (next-method query-type model parsed-args sql-args))

;;; default implementation of [[build]] for maps lives in [[toucan2.honeysql2]]

(m/defmulti resolve
  "Resolve a `queryable` to an actual query, e.g. resolve a named query defined by [[toucan2.tools.named-query]] to an
  actual Honey SQL map."
  {:arglists            '([query-type₁ model₂ queryable₃])
   :defmethod-arities   #{3}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type-model-query)}
  u/dispatch-on-first-three-args)

(m/defmethod resolve :default
  "The default implementation considers a query to already be resolved, and returns it as-is."
  [_query-type _model queryable]
  queryable)

(def ^:dynamic ^{:arglists '([query-type model parsed-args resolved-query])} *build*
  "The function to use when building a query. Normally [[build]], but you can bind this to intercept build behavior to
  do something different."
  #'build)

(def ^:dynamic ^{:arglists '([query-type model built-query])} *compile*
  "The function to use when compiling a query. Normally [[compile]], but you can bind this to intercept normal
  compilation behavior to do something different."
  #'compile)

(def ^:no-doc ^:dynamic ^{:arglists '([rf query-type model compiled-query])}
  *transduce-execute*
  "The function to use to open a connection, execute, and transduce a query. Normally [[transduce-execute]]. The primary
  use case for binding this is to intercept query execution and return some results without opening any connections."
  #'transduce-execute)

(def ^:dynamic *parsed-args*
  "The parsed args seen at the beginning of the pipeline. This is bound in case methods in later stages of the pipeline,
  such as [[results-transform]], need it for one reason or another. (See for example [[toucan2.tools.default-fields]],
  which applies different behavior if a query was initiated with `[model & columns]` syntax vs. if it was not.)"
  nil)

(def ^:dynamic *resolved-query*
  "The query after it has been resolved. This is bound in case methods in the later stages of the pipeline need it for one
  reason or another."
  nil)

(defn- transduce-compiled-query [rf query-type model compiled-query]
  (u/try-with-error-context ["with compiled query" {::compiled-query compiled-query}]
    (let [xform (results-transform query-type model)
          rf    (xform rf)]
      (*transduce-execute* rf query-type model compiled-query))))

(defn- transduce-built-query [rf query-type model built-query]
  (u/try-with-error-context ["with built query" {::built-query built-query}]
    (if (isa? built-query ::no-op)
      (let [init (rf)]
        (rf init))
      (let [compiled-query (*compile* query-type model built-query)]
        (transduce-compiled-query rf query-type model compiled-query)))))

(m/defmulti transduce-query
  "One of the primary customization points for the Toucan 2 query execution pipeline. [[build]] and [[compile]] a
  `resolved-query`, then open a connection, execute the query, and transduce the results
  with [[transduce-execute-with-connection]] (using the [[results-transform]]).

  You can implement this method to introduce custom behavior that should happen before a query is built or compiled,
  e.g. transformations to the `parsed-args` or other shenanigans like changing underlying query type being
  executed (e.g. [[toucan2.tools.after]], which 'upgrades' queries returning update counts or PKs to ones returning
  instances so [[toucan2.tools.after-update]] and [[toucan2.tools.after-insert]] can be applied to affected rows)."
  {:arglists            '([rf query-type₁ model₂ parsed-args resolved-query₃])
   :defmethod-arities   #{5}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type-model-query)}
  (fn [_rf query-type model _parsed-args resolved-query]
    (u/dispatch-on-first-three-args query-type model resolved-query)))

(m/defmethod transduce-query :default
  [rf query-type model parsed-args resolved-query]
  (let [built-query (*build* query-type model parsed-args resolved-query)]
    (transduce-built-query rf query-type model built-query)))

(defn- transduce-query* [rf query-type model parsed-args resolved-query]
  (let [parsed-args (dissoc parsed-args :queryable)]
    (binding [*resolved-query* resolved-query]
      (u/try-with-error-context ["with resolved query" {::resolved-query resolved-query}]
        (transduce-query rf query-type model parsed-args resolved-query)))))

(defn- transduce-with-model
  [rf query-type model {:keys [queryable], :as parsed-args}]
  ;; if `*current-connectable*` is unbound but `model` has a default connectable, bind `*current-connectable*` and recur
  (if-let [model-connectable (when-not conn/*current-connectable*
                               (model/default-connectable model))]
    (binding [conn/*current-connectable* model-connectable]
      (transduce-with-model rf query-type model parsed-args))
    (binding [*parsed-args* parsed-args]
      (u/try-with-error-context ["with parsed args" {::query-type query-type, ::parsed-args parsed-args}]
        (let [queryable      (if (contains? parsed-args :queryable)
                               queryable
                               (or queryable {}))
              resolved-query (resolve query-type model queryable)]
          (transduce-query* rf query-type model parsed-args resolved-query))))))

(defn ^:no-doc transduce-parsed
  "Like [[transduce-unparsed]], but called with already-parsed args rather than unparsed args."
  [rf query-type {:keys [modelable connectable], :as parsed-args}]
  ;; if `:connectable` was specified, bind it to [[conn/*current-connectable*]]; it should always override the current
  ;; connection (if one is bound). See docstring for [[toucan2.query/reducible-query]] for more info.
  ;;
  ;; TODO -- I'm not 100% sure this makes sense -- if we specify `:conn ::my-connection` and then want to do something
  ;; in a transaction for `::my-connection`? Shouldn't it still be done in a transaction?
  (if connectable
    (binding [conn/*current-connectable* connectable]
      (transduce-parsed rf query-type (dissoc parsed-args :connectable)))
    ;; if [[conn/*current-connectable*]] is not yet bound, then get the default connectable for the model and recur.
    (let [model (model/resolve-model modelable)]
      (u/try-with-error-context ["with model" {::model model}]
        (transduce-with-model rf query-type model (dissoc parsed-args :modelable))))))

(defn ^:no-doc transduce-unparsed
  "Entrypoint to the Toucan 2 query execution pipeline. Parse `unparsed-args` for a `query-type`, then resolve model and
  query, build and compile query, then open a connection, execute the query, and transduce the results."
  [rf query-type unparsed-args]
  (let [parsed-args (query/parse-args query-type unparsed-args)]
    (u/try-with-error-context ["with unparsed args" {::query-type query-type, ::unparsed-args unparsed-args}]
      (transduce-parsed rf query-type parsed-args))))

;;;; rf helper functions

(defn ^:no-doc with-init
  "Returns a version of reducing function `rf` with a zero-arity (initial value arity) that returns `init`."
  [rf init]
  (fn
    ([]    init)
    ([x]   (rf x))
    ([x y] (rf x y))))

(defn ^:no-doc conj-with-init!
  "Returns a reducing function with a zero-arity (initial value arity) that returns transient version of `init`, `conj!`s
  values into it, and finally returns a persistent collection in 1-arity."
  [init]
  (fn
    ([]      (transient init))
    ([acc]   (persistent! acc))
    ([acc y] (conj! acc y))))

(m/defmulti default-rf
  "The default reducing function for queries of `query-type`. Used for non-reducible operations
  like [[toucan2.select/select]] or [[toucan2.execute/query]]."
  {:arglists            '([query-type])
   :defmethod-arities   #{1}
   :dispatch-value-spec (s/nonconforming ::types/dispatch-value.query-type)}
  keyword)

(m/defmethod default-rf :toucan.result-type/update-count
  "The reducing function for queries returning an update count. Sums all numbers passed in."
  [_query-type]
  (-> (fnil + 0 0)
      (with-init 0)
      completing))

(m/defmethod default-rf :toucan.result-type/pks
  "The reducing function for queries returning PKs. Presumably these will come back as a map, but that map doesn't need to
  be realized. This needs to be combined with a transducer like `map` [[toucan2.model/select-pks-fn]] to get the PKs
  themselves."
  [_query-type]
  conj)

(m/defmethod default-rf :toucan.result-type/*
  "The default reducing function for all query types unless otherwise specified. Returns realized maps (by default, Toucan
  2 instances)."
  [_query-type]
  ((map realize/realize) conj))

(defn ^:no-doc first-result-xform-fn
  "Return a transducer that transforms a reducing function `rf` so it always takes at most one value and returns the first
  value from the results. This doesn't work for things that return update counts!"
  [query-type]
  (if (isa? query-type :toucan.result-type/update-count)
    identity
    (fn [rf]
      (completing ((take 1) rf) first))))

;;;; Helper functions for implementing stuff like [[toucan2.select/select]]

(defn ^:no-doc transduce-unparsed-with-default-rf
  "Helper for implementing things like [[toucan2.select/select]]. Transduce `unparsed-args` using the [[default-rf]] for
  this `query-type`."
  [query-type unparsed-args]
  (assert (types/query-type? query-type))
  (let [rf (default-rf query-type)]
    (transduce-unparsed rf query-type unparsed-args)))


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

(defn ^:no-doc reducible-unparsed
  "Helper for implementing things like [[toucan2.select/reducible-select]]. A reducible version
  of [[transduce-unparsed]]."
  [query-type unparsed]
  (reducible-fn transduce-unparsed query-type unparsed))

(defn ^:no-doc reducible-parsed-args
  "Helper for implementing things like [[toucan2.execute/reducible-query]] that don't need arg parsing. A reducible
  version of [[transduce-parsed]]."
  [query-type parsed-args]
  (reducible-fn transduce-parsed query-type parsed-args))

;;;; Misc util functions. TODO -- I don't think this belongs here; hopefully this can live somewhere where we can call
;;;; it `compile` instead.

(defn compile*
  "Helper for compiling a `built-query` to something that can be executed natively."
  ([built-query]
   (compile* nil built-query))
  ([query-type built-query]
   (compile* query-type nil built-query))
  ([query-type model built-query]
   (compile query-type model built-query)))
