(ns toucan2.pipeline
  "This is a low-level namespace implementing our query execution pipeline. Most of the stuff you'd use on a regular basis
  are implemented on top of stuff here.

  Pipeline order is

  1. [[parse-args]]                  (entrypoint fn: [[transduce-unparsed]])
  2. [[toucan2.model/resolve-model]] (entrypoint fn: [[transduce-parsed]])
  3. [[transduce-with-model]]        ; TODO -- `resolve-model` + transduce query?
  4. [[resolve]]
  5. [[build]]                       ; TODO -- we should introduce a new hook here to use instead of [[transduce-with-model]]
  6. [[compile]]
  x. [[results-transform]]
  7. [[transduce-execute]]           ; TODO `with-connection`
  8. [[transduce-execute-with-connection]]

  The main pipeline entrypoint is [[transduce-unparsed]]."
  (:refer-clojure :exclude [compile resolve])
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.jdbc :as jdbc]
   [toucan2.jdbc.query :as jdbc.query]
   [toucan2.map-backend :as map]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.types :as types]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

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

(m/defmulti transduce-execute-with-connection
  {:arglists '([rf conn₁ query-type₂ model₃ compiled-query]), :defmethod-arities #{5}}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

;;;; default JDBC impls. Not convinced they belong here.

(m/defmethod transduce-execute-with-connection [#_connection java.sql.Connection
                                                #_query-type :default
                                                #_model      :default]
  [rf conn query-type model sql-args]
  {:pre [(sequential? sql-args) (string? (first sql-args))]}
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
    (binding [jdbc/*options* (assoc jdbc/*options* :return-keys true)]
      (next-method rf* conn query-type model sql-args))))

(m/defmulti transduce-execute
  {:arglists '([rf query-type₁ model₂ compiled-query₃]), :defmethod-arities #{4}}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

(m/defmethod transduce-execute :default
  [rf query-type model compiled-query]
  (conn/with-connection [conn]
    (transduce-execute-with-connection rf conn query-type model compiled-query)))

(m/defmethod transduce-execute [#_query-type :toucan.statement-type/DML #_model :default #_compiled-query :default]
  "For DML stuff we will run the whole thing in a transaction if we're not already in one.

  Not 100% sure this is necessary since we would probably already be in one if we needed to be because stuff
  like [[toucan2.tools.before-delete]] have to put us in one much earlier."
  [rf query-type model compiled-query]
  (conn/with-transaction [_conn nil {:nested-transaction-rule :ignore}]
    (next-method rf query-type model compiled-query)))

(m/defmulti results-transform
  {:arglists '([query-type₁ model₂]), :defmethod-arities #{2}}
  u/dispatch-on-first-two-args)

(m/defmethod results-transform :default
  [_query-type _model]
  identity)

(m/defmulti compile
  {:arglists '([query-type₁ model₂ built-query₃]), :defmethod-arities #{3}}
  u/dispatch-on-first-three-args)

(m/defmethod compile :default
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

(m/defmethod compile [#_query-type :default #_model :default #_built-query clojure.lang.IPersistentMap]
  "Default method for a map. Hands off to the appropriate method for [[toucan2.map-backend/backend]]."
  [query-type model m]
  (compile query-type model (vary-meta m assoc :type (map/backend))))

(m/defmulti build
  {:arglists '([query-type₁ model₂ parsed-args resolved-query₃]), :defmethod-arities #{4}}
  (fn [query-type₁ model₂ _parsed-args resolved-query₃]
    (u/dispatch-on-first-three-args query-type₁ model₂ resolved-query₃)))

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

(m/defmethod build [#_query-type :default #_model :default #_resolved-query :toucan.map-backend/*]
  "Base map backend implementation."
  [query-type model {:keys [kv-args], :as parsed-args} m]
  (let [m (query/apply-kv-args model m kv-args)]
    (next-method query-type model (dissoc parsed-args :kv-args) m)))

(m/defmethod build [#_query-type :default #_model :default #_resolved-query clojure.lang.IPersistentMap]
  "Default implementation for maps. Calls the [[toucan2.map-backend/backend]] method."
  [query-type model parsed-args m]
  (build query-type model parsed-args (vary-meta m assoc :type (map/backend))))

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

(m/defmulti resolve
  {:arglists '([query-type₁ model₂ queryable₃]), :defmethod-arities #{3}}
  u/dispatch-on-first-three-args)

(m/defmethod resolve :default
  [_query-type _model queryable]
  queryable)

;;;; [[transduce-with-model]]

(m/defmulti transduce-with-model
  {:arglists '([rf query-type₁ model₂ parsed-args]), :defmethod-arities #{4}}
  (dispatch-ignore-rf u/dispatch-on-first-two-args))

(def ^:dynamic ^{:arglists '([query-type model parsed-args resolved-query])}
  *build*
  #'build)

(def ^:dynamic ^{:arglists '([query-type model built-query])}
  *compile*
  #'compile)

(def ^:dynamic ^{:arglists '([rf query-type model compiled-query])}
  *transduce-execute*
  #'transduce-execute)

(def ^:dynamic *parsed-args*
  "This is here just in case you might happen to need it for methods that aren't normally called with it."
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

(defn- transduce-resolved-query [rf query-type model parsed-args resolved-query]
  (u/try-with-error-context ["with resolved query" {::resolved-query resolved-query}]
    (let [parsed-args (dissoc parsed-args :queryable)
          built-query (*build* query-type model parsed-args resolved-query)]
      (transduce-built-query rf query-type model built-query))))

(m/defmethod transduce-with-model :default
  [rf query-type model {:keys [queryable], :as parsed-args}]
  (binding [*parsed-args* parsed-args]
    (u/try-with-error-context ["with parsed args" {::query-type query-type, ::parsed-args parsed-args}]
      (let [queryable      (if (contains? parsed-args :queryable)
                             queryable
                             (or queryable {}))
            resolved-query (resolve query-type model queryable)]
        (transduce-resolved-query rf query-type model parsed-args resolved-query)))))

(defn- transduce-with-model* [rf query-type model parsed-args]
  (if-let [model-connectable (when-not conn/*current-connectable*
                               (model/default-connectable model))]
    (binding [conn/*current-connectable* model-connectable]
      (transduce-with-model* rf query-type model parsed-args))
    (transduce-with-model rf query-type model parsed-args)))

(defn transduce-parsed
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
        (transduce-with-model* rf query-type model (dissoc parsed-args :modelable))))))

(m/defmulti parse-args
  {:arglists '([query-type₁ unparsed-args]), :defmethod-arities #{2}}
  u/dispatch-on-first-arg)

#_{:clj-kondo/ignore [:redundant-fn-wrapper]}
(m/defmethod parse-args :default
  [query-type unparsed-args]
  (query/parse-args query-type unparsed-args))

(defn transduce-unparsed
  [rf query-type unparsed-args]
  (let [parsed-args (parse-args query-type unparsed-args)]
    (u/try-with-error-context ["with unparsed args" {::query-type query-type, ::unparsed-args unparsed-args}]
      (transduce-parsed rf query-type parsed-args))))

;;;; rf helper functions

(defn with-init
  "Returns a version of reducing function `rf` with a zero-arity (initial value arity) that returns `init`."
  [rf init]
  (fn
    ([]    init)
    ([x]   (rf x))
    ([x y] (rf x y))))

(m/defmulti default-rf
  {:arglists '([query-type]),:defmethod-arities #{1}}
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
  (assert (types/query-type? query-type))
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

(defn- DML? [query-type]
  (isa? query-type :toucan.statement-type/DML))

(m/defmethod transduce-execute-with-connection [#_connection java.sql.Connection
                                                #_query-type :toucan.result-type/instances
                                                #_model      :default]
  [rf conn query-type model sql-args]
  ;; for non-DML stuff (ie `SELECT`) JDBC can actually return instances with zero special magic, so we can just let the
  ;; next method do it's thing. Presumably if we end up here with something that is neither DML or DQL, but maybe
  ;; something like a DDL `CREATE TABLE` statement, we probably don't want to assume it has the possibility to have it
  ;; return generated PKs.
  (let [pk-query-type (when DML?
                        (types/similar-query-type-returning query-type :toucan.result-type/pks))]
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
            columns (:columns *parsed-args*)]
        ;; once we have a sequence of PKs then get instances as with `select` and do our magic on them using the
        ;; ORIGINAL `rf`.
        (transduce-instances-from-pks rf model columns pks)))))

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
