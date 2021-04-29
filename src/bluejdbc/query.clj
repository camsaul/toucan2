(ns bluejdbc.query
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [next.jdbc.result-set :as next.jdbc.rs]
            [potemkin :as p]
            [pretty.core :as pretty]))

;; TODO -- should this be off by default?
(def ^:dynamic *include-queries-in-exceptions?* true)

(defn- reduce-query
  [connectable tableable queryable options rf init]
  (let [options    (u/recursive-merge (conn/default-options connectable) options)
        sql-params (compile/compile connectable tableable queryable options)]
    (conn/with-connection [conn connectable options]
      (try
        (log/tracef "Executing query %s with options %s" (pr-str sql-params) (pr-str (:execute options)))
        (let [results (next.jdbc/plan conn sql-params (:execute options))]
          (try
            (log/tracef "Reducing results with rf %s and init %s" (pr-str rf) (pr-str init))
            (reduce rf init results)
            (catch Throwable e
              (println "e:" e) ; NOCOMMIT
              (let [message (or (:message (ex-data e)) (ex-message e))]
                (throw (ex-info (format "Error reducing results: %s" message)
                                {:rf rf, :init init, :message message}
                                e))))))
        (catch Throwable e
          (println "e [2]:" e) ; NOCOMMIT
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
    (list `reducible-query connectable tableable queryable options))

  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce-query connectable tableable queryable options rf init))

  #_clojure.lang.ISeq
  #_(seq [this]
    (seq (all this))))

(m/defmulti reducible-query*
  {:arglists '([connectable tableable queryable options])}
  u/dispatch-on-first-three-args)

(m/defmethod reducible-query* :default
  [connectable tableable queryable options]
  (->ReducibleQuery connectable tableable queryable options))

(defn reducible-query
  ([queryable]
   (reducible-query conn/*connectable* nil queryable nil))

  ([connectable queryable]
   (reducible-query connectable nil queryable nil))

  ([connectable tableable queryable]
   (reducible-query connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (reducible-query* connectable tableable queryable (u/recursive-merge (conn/default-options connectable) options))))

(defn realize-row [row]
  (next.jdbc.rs/datafiable-row row conn/*connection* nil))

(defn all [reducible-query]
  (into
   []
   (map realize-row)
   reducible-query))

(defn query
  [& args]
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
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
  (conn/with-connection [conn connectable options]
    (let [sql-params (compile/compile connectable tableable query options)]
      (try
        (next.jdbc/execute! conn sql-params (:execute options))
        (catch Throwable e
          (throw (ex-info "Error executing statement"
                          (merge
                           {:options options}
                           (when *include-queries-in-exceptions?*
                             {:query query, :sql-params sql-params}))
                          e)))))))

(defn execute!
  ([query]                               (execute!  :default    nil       query nil))
  ([connectable query]                   (execute!  connectable nil       query nil))
  ([connectable query options]           (execute!  connectable nil       query nil))
  ([connectable tableable query options] (execute!* connectable tableable query (merge (conn/default-options connectable)
                                                                                       options))))
