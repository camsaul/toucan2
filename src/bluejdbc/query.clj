(ns bluejdbc.query
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.log :as log]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.result-set :as next.jdbc.rs]
            [potemkin :as p]
            [pretty.core :as pretty]))

;; TODO -- should this be off by default?
(def ^:dynamic *include-queries-in-exceptions?* true)

(def ^:dynamic ^:private *call-count-thunk* (fn [])) ; no-op

(def ^:dynamic ^:private *return-compiled* false)

(def ^:dynamic ^:private *return-uncompiled* false)

(defn do-compiled [thunk]
  (binding [*return-compiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro compiled {:style/indent 0} [& body]
  `(do-compiled (fn [] ~@body)))

(defn do-uncompiled [thunk]
  (binding [*return-uncompiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro uncompiled {:style/indent 0} [& body]
  `(do-uncompiled (fn [] ~@body)))

(defn- compile [connectable tableable queryable options]
  (when *return-uncompiled*
    (throw (u/quit-early-exception queryable)))
  (let [sql-params (compile/compile connectable tableable queryable options)]
    (when *return-compiled*
      (throw (u/quit-early-exception sql-params)))
    sql-params))

(defn- reduce-query
  [connectable tableable queryable options rf init]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        sql-params            (compile connectable tableable queryable options)]
    (conn/with-connection [conn connectable tableable options]
      (try
        (log/with-trace ["Executing query %s with options %s" sql-params (:next.jdbc options)]
          (let [[sql & params] sql-params
                params         (for [param params]
                                 (stmt/parameter connectable tableable param options))
                sql-params     (cons sql params)
                results        (next.jdbc/plan conn sql-params (:next.jdbc options))]
            (try
              (log/with-trace ["Reducing results with rf %s and init %s" rf init]
                (*call-count-thunk*)
                (reduce rf init results))
              (catch Throwable e
                (let [message (or (:message (ex-data e)) (ex-message e))]
                  (throw (ex-info (format "Error reducing results: %s" message)
                                  {:rf rf, :init init, :message message}
                                  e)))))))
        (catch Throwable e
          (let [message (or (:message (ex-data e)) (ex-message e))]
            (throw (ex-info (format "Error executing query: %s" message)
                            (merge
                             {:options options
                              :message message}
                             (when *include-queries-in-exceptions?*
                               {:query queryable, :sql-params sql-params, :options options}))
                            e))))))))

(declare all)

(p/deftype+ ReducibleQuery [connectable tableable queryable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list (u/qualify-symbol-for-*ns* `reducible-query) connectable tableable queryable options))

  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce-query connectable tableable queryable options rf init)))

(m/defmulti reducible-query*
  {:arglists '([connectable tableable queryable options])}
  u/dispatch-on-first-three-args)

(m/defmethod reducible-query* :default
  [connectable tableable queryable options]
  (assert (some? connectable) "connectable should not be nil; use current-connectable to get the connectable to use")
  (->ReducibleQuery connectable tableable queryable options))

(defn reducible-query
  ([queryable]
   (reducible-query nil nil queryable nil))

  ([connectable queryable]
   (reducible-query connectable nil queryable nil))

  ([connectable tableable queryable]
   (reducible-query connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (reducible-query* connectable tableable queryable options))))

(defn realize-row [row]
  (next.jdbc.rs/datafiable-row row conn.current/*current-connection* nil))

(defn all [reducible-query]
  (into
   []
   (map realize-row)
   reducible-query))

(defn query
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (all (apply reducible-query args)))

(defn reduce-first
  ([reducible]
   (reduce-first identity reducible))

  ([xform reducible]
   (transduce
    (comp xform (take 1))
    (completing conj first)
    []
    reducible)))

(defn query-one
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (reduce-first (map realize-row) (apply reducible-query args)))

(m/defmulti execute!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod execute!* :default
  [connectable tableable query options]
  (conn/with-connection [conn connectable tableable options]
    (let [sql-params (compile connectable tableable query options)]
      (try
        (*call-count-thunk*)
        (next.jdbc/execute! conn sql-params (:next.jdbc options))
        (catch Throwable e
          (throw (ex-info (format "Error executing statement: %s" (ex-message e))
                          (merge
                           {:options options}
                           (when *include-queries-in-exceptions?*
                             {:query query, :sql-params sql-params}))
                          e)))))))

(defn execute!
  ([query]                       (execute!  nil         nil       query nil))
  ([connectable query]           (execute!  connectable nil       query nil))
  ([connectable tableable query] (execute!  connectable tableable query nil))

  ([connectable tableable query options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (execute!* connectable tableable query options))))

(defn do-with-call-counts [f]
  (let [call-count (atom 0)
        old-thunk  *call-count-thunk*]
    (binding [*call-count-thunk* #(do
                                    (old-thunk)
                                    (swap! call-count inc))]
      (f (fn [] @call-count)))))

(defmacro with-call-count [[call-count-fn-binding] & body]
  `(do-with-call-counts (fn [~call-count-fn-binding] ~@body)))
