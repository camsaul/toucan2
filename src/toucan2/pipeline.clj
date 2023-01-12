(ns toucan2.pipeline
  "This is a low-level namespace implementing our query execution pipeline. Most of the stuff you'd use on a regular basis
  are implemented on top of stuff here.

  Pipeline order is

  1. [[transduce-unparsed]] ; TODO `parse-args`
  2. [[transduce-parsed]]   ; TODO `resolve-model`
  3. [[transduce-with-model]]
  4. [[resolve]]
  5. [[build]]
  6. [[compile]]
  7. [[transduce-execute]]  ; TODO `with-connection`
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

;;;; [[transduce-execute-with-connection]]

(m/defmulti transduce-execute-with-connection
  {:arglists '([rf conn₁ query-type₂ model₃ compiled-query]), :defmethod-arities #{5}}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

;;;; [[transduce-execute]]

(m/defmulti transduce-execute
  {:arglists '([rf query-type₁ model₂ compiled-query₃]), :defmethod-arities #{4}}
  (dispatch-ignore-rf u/dispatch-on-first-three-args))

;;; TODO -- see if we can eliminate this by doing it directly in one of the pipeline methods.
(defn- current-connectable [model]
  (or conn/*current-connectable*
      (model/default-connectable model)))

(m/defmethod transduce-execute :default
  [rf query-type model compiled-query]
  (conn/with-connection [conn (current-connectable model)]
    (transduce-execute-with-connection rf conn query-type model compiled-query)))

(m/defmethod transduce-execute [#_query-type :toucan.statement-type/DML #_model :default #_compiled-query :default]
  "For DML stuff we will run the whole thing in a transaction if we're not already in one.

  Not 100% sure this is necessary since we would probably already be in one if we needed to be because stuff
  like [[toucan2.tools.before-delete]] have to put us in one much earlier."
  [rf query-type model compiled-query]
  (conn/with-transaction [_conn (current-connectable model) {:nested-transaction-rule :ignore}]
    (next-method rf query-type model compiled-query)))

(m/defmulti compile
  {:arglists '([query-type₁ model₂ built-query₃]), :defmethod-arities #{3}}
  u/dispatch-on-first-three-args)

;;; HACK
(def ^:private ^:dynamic *built-query* nil)

(m/defmethod compile :default
  [_query-type _model query]
  (assert (and (some? query)
               (or (not (coll? query))
                   (seq query)))
          (format "Compiled query should not be nil/empty. Got: %s" (pr-str query)))
  (vary-meta query (fn [metta]
                     (merge (dissoc (meta *built-query*) :type) metta))))

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

(m/defmethod transduce-with-model :default
  [rf query-type model {:keys [queryable], :as parsed-args}]
  (let [queryable      (if (contains? parsed-args :queryable)
                         queryable
                         (or queryable {}))
        resolved-query (resolve query-type model queryable)
        parsed-args    (dissoc parsed-args :queryable)
        built-query    (*build* query-type model parsed-args resolved-query)]
    (if (isa? built-query ::no-op)
      (let [init (rf)]
        (rf init))
      (let [compiled-query (*compile* query-type model built-query)]
        (*transduce-execute* rf query-type model compiled-query)))))

(m/defmulti transduce-parsed
  {:arglists '([rf query-type₁ parsed-args])}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

(m/defmethod transduce-parsed :default
  [rf query-type {:keys [modelable connectable], :as parsed-args}]
  (let [model (model/resolve-model modelable)
        thunk (^:once fn* []
               (transduce-with-model rf query-type model (dissoc parsed-args :modelable :connectable)))]
    (if connectable
      (binding [conn/*current-connectable* connectable]
        (thunk))
      (thunk))))

(m/defmulti transduce-unparsed
  {:arglists '([rf query-type₁ unparsed-args]), :defmethod-arities #{3}}
  (dispatch-ignore-rf u/dispatch-on-first-arg))

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

(def ^:private ^:dynamic *parsed-args* nil)

(m/defmethod transduce-with-model [#_query-type :toucan.result-type/instances #_model :default]
  [rf query-type model parsed-args]
  (if (DML? query-type)
    (binding [*parsed-args* parsed-args]
      (next-method rf query-type model parsed-args))
    (next-method rf query-type model parsed-args)))

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
