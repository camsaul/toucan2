(ns bluejdbc.query
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.result-set :as rs]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as jdbc]
            [pretty.core :as pretty]))

(def ^:dynamic *include-queries-in-exceptions* true)

(m/defmulti query*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod query* :default
  [connectable tableable query options]
  (reify
    pretty/PrettyPrintable
    (pretty [_]
      (list `query connectable tableable query options))

    clojure.lang.IReduceInit
    (reduce [_ rf init]
      (conn/with-connection [conn connectable options]
        (let [sql-params (compile/compile connectable tableable query options)]
          (try
            (log/tracef "executing query %s with options %s" (pr-str sql-params) (pr-str (:execute options)))
            (let [results (jdbc/plan conn sql-params (:execute options))]
              (try
                (reduce rf init results)
                (catch Throwable e
                  (throw (ex-info "Error reducing results"
                                  {:rf rf, :init init, :options options}
                                  e)))))
            (catch Throwable e
              (throw (ex-info "Error executing query"
                              (merge
                               {:options options}
                               (when *include-queries-in-exceptions*
                                 {:query query, :sql-params sql-params}))
                              e)))))))

    clojure.lang.IReduce
    (reduce [this rf]
      (reduce rf [] this))))

(defn query
  ([qury]                                (query  :default    nil       qury nil))
  ([connectable qury]                    (query  connectable nil       qury nil))
  ([connectable qury options]            (query  connectable nil       qury options))
  ([connectable tableable qury options]  (query* connectable tableable qury (merge (conn/default-options connectable)
                                                                                   {:execute {:builder-fn (rs/row-builder-fn connectable tableable)}}
                                                                                   options))))

(defn- ^:deprecated default-rf
  ([]
   [])

  ([acc]
   acc)

  ([acc row]
   (conj acc (into {} row))))

(m/defmulti query-all*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod query-all* :default
  [connectable tableable query options]
  (reduce
   (or (:rf options) default-rf)
   (query* connectable tableable query options)))

(defn query-all
  ([query]                               (query-all  :default    nil       query nil))
  ([connectable query]                   (query-all  connectable nil       query nil))
  ([connectable query options]           (query-all  connectable nil       query options))
  ([connectable tableable query options] (query-all* connectable tableable query (merge (conn/default-options connectable)
                                                                                        options))))

(m/defmulti query-one*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-three-args)

(defn- reduce-first [rf reducible]
  (transduce (take 1) (completing rf first) [] reducible))

(m/defmethod query-one* :default
  [connectable tableable query options]
  (reduce-first (or (:rf options) default-rf) (query* connectable tableable query options)))

(defn query-one
  ([query]                               (query-one  :default    nil       query nil))
  ([connectable query]                   (query-one  connectable nil       query nil))
  ([connectable query options]           (query-one  connectable nil       query options))
  ([connectable tableable query options] (query-one* connectable tableable query (merge (conn/default-options connectable)
                                                                                options))))

(m/defmulti execute!*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod execute!* :default
  [connectable tableable query options]
  (conn/with-connection [conn connectable options]
    (let [sql-params (compile/compile connectable tableable query options)]
      (try
        (jdbc/execute! conn sql-params (:execute options))
        (catch Throwable e
          (throw (ex-info "Error executing statement"
                          (merge
                           {:options options}
                           (when *include-queries-in-exceptions*
                             {:query query, :sql-params sql-params}))
                          e)))))))

(defn execute!
  ([query]                               (execute!  :default    nil       query nil))
  ([connectable query]                   (execute!  connectable nil       query nil))
  ([connectable query options]           (execute!  connectable nil       query nil))
  ([connectable tableable query options] (execute!* connectable tableable query (merge (conn/default-options connectable)
                                                                                       options))))
