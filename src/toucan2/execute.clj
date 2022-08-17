(ns toucan2.execute
  "Code for executing queries and statements, and reducing their results."
  (:refer-clojure :exclude [compile])
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

;;;; Reducible query and pipeline.

;;; Basic execution pipeline is something this
;;;
;;; [[reduce-impl]]
;;; resolve `modelable` => `model` with `[[model/with-model]]
;;; resolve `queryable` => `query` with [[compile/with-query]]
;;;
;;; [[reduce-uncompiled-query]]
;;; Compile `query` => `compile-query` with [[compile/with-compiled-query]]
;;;
;;; [[reduce-compiled-query]]
;;; Open `conn` from `connectable` with [[conn/with-connection]]
;;;
;;; [[reduce-compiled-query-with-connection]]
;;; Execute and reduce the query with the open connection.

(def ^:dynamic *call-count-thunk*
  "Thunk function to call every time a query is executed if [[with-call-count]] is in use."
  nil)

(m/defmulti reduce-compiled-query-with-connection
  {:arglists '([conn model compiled-query rf init])}
  u/dispatch-on-first-three-args)

(m/defmethod reduce-compiled-query-with-connection :before :default
  [_conn _model _compiled-query _rf init]
  (when *call-count-thunk*
    (*call-count-thunk*))
  init)

(m/defmethod reduce-compiled-query-with-connection [java.sql.Connection :default clojure.lang.Sequential]
  [conn model sql-args rf init]
  (t2.jdbc.query/reduce-jdbc-query conn model sql-args rf init))

(m/defmulti reduce-compiled-query
  {:arglists '([connectable model compiled-query rf init])}
  (fn [_connectable model compiled-query _rf _init]
    [(u/dispatch-value model) (u/dispatch-value compiled-query)]))

(m/defmethod reduce-compiled-query :around :default
  [connectable model compiled-query rf init]
  (try
    (next-method connectable model compiled-query rf init)
    (catch Throwable e
      (throw (ex-info (str "Error reducing compiled query: " (ex-message e))
                      {:model model, :compiled-query compiled-query}
                      e)))))

(m/defmethod reduce-compiled-query :default
  [connectable model compiled-query rf init]
  (conn/with-connection [conn connectable]
    (reduce-compiled-query-with-connection conn model compiled-query rf init)))

(m/defmulti reduce-uncompiled-query
  {:arglists '([connectable model query rf init])}
  (fn [_connectable model query _rf _init]
    [(u/dispatch-value model) (u/dispatch-value query)]))

(m/defmethod reduce-uncompiled-query :default
  [connectable model query rf init]
  (compile/with-compiled-query [compiled-query [model query]]
    (reduce-compiled-query connectable model compiled-query rf init)))

(defn- reduce-impl [connectable modelable queryable rf init]
  (model/with-model [model modelable]
    (query/with-query [query [model queryable]]
      (reduce-uncompiled-query connectable model query rf init))))

(defrecord ReducibleQuery [connectable modelable queryable]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce-impl connectable modelable queryable rf init))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable modelable queryable)))

(defn reducible-query
  ([queryable]
   (reducible-query ::conn/current queryable))
  ([connectable queryable]
   (reducible-query connectable nil queryable))
  ([connectable modelable queryable]
   (->ReducibleQuery connectable modelable queryable)))

;;;; Util functions for running queries and immediately realizing the results.

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable])}
  query
  (comp realize/realize reducible-query))

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable modelable queryable])}
  query-one
  (comp realize/reduce-first reducible-query))

;;; No need for a separate `execute` function anymore -- you can use [[query]] for all the same stuff.

;;;; [[compile]]

;;; TODO -- `execute/compile` seems a little weird. Should this go somewhere else maybe?

(m/defmethod conn/do-with-connection ::compile
  [connectable f]
  (f connectable))

(m/defmethod reduce-compiled-query-with-connection [::compile :default :default]
  [_connectable _model compiled-query rf init]
  (reduce rf init [{::query compiled-query}]))

(defmacro compile
  "Return the compiled query that would be executed by a form, rather than executing that form itself.

    (delete/delete :table :id 1)
    =>
    [\"DELETE FROM table WHERE ID = ?\" 1]"
  {:style/indent 0}
  [& body]
  `(binding [current/*connection* ::compile]
     (let [query# (do ~@body)]
       (::query (realize/reduce-first query#)))))

;;;; [[with-call-count]]

(defn do-with-call-counts
  "Impl for [[with-call-count]] macro; don't call this directly."
  [f]
  (let [call-count (atom 0)
        old-thunk  *call-count-thunk*]
    (binding [*call-count-thunk* (fn []
                                   (when old-thunk
                                     (old-thunk))
                                   (swap! call-count inc))]
      (f (fn [] @call-count)))))

(defmacro with-call-count
  "Execute `body`, trackingthe number of database queries and statements executed. This number can be fetched at any
  time withing `body` by calling function bound to `call-count-fn-binding`:

    (with-call-count [call-count]
      (select ...)
      (println \"CALLS:\" (call-count))
      (insert! ...)
      (println \"CALLS:\" (call-count)))
    ;; -> CALLS: 1
    ;; -> CALLS: 2"
  [[call-count-fn-binding] & body]
  `(do-with-call-counts (^:once fn* [~call-count-fn-binding] ~@body)))

;;; TODO -- this is kind of JDBC specific
(defrecord WithReturnKeys [reducible]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (binding [t2.jdbc.query/*options* (assoc t2.jdbc.query/*options* :return-keys true)]
      (reduce rf init reducible)))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `->WithReturnKeys reducible)))
