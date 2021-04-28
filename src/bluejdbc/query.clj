(ns bluejdbc.query
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [potemkin :as p]))

(defn reduce-query
  [connectable
   query
   {:keys [include-queries-in-exceptions?]
    :or   {include-queries-in-exceptions? true}
    :as   options}
   rf
   init]
  (conn/with-connection [[conn options] connectable options]
    (let [sql-params (compile/compile connectable query options)]
      (try
        (log/tracef "executing query %s with options %s" (pr-str sql-params) (pr-str (:execute options)))
        (let [results (next.jdbc/plan conn sql-params (:execute options))]
          (try
            (reduce rf init results)
            (catch Throwable e
              (throw (ex-info "Error reducing results" {:rf rf, :init init} e)))))
        (catch Throwable e
          (throw (ex-info "Error executing query"
                          (merge
                           {:options options}
                           (when include-queries-in-exceptions?
                             {:query query, :sql-params sql-params, :options options}))
                          e)))))))

(p/defrecord+ ReducibleQuery [connectable query options]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce-query connectable query options rf init))

  clojure.lang.IReduce
  (reduce [_ rf]
    (reduce-query connectable query options rf (:init options []))))

(m/defmulti reducible-query*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(m/defmethod reducible-query* :default
  [connectable query options]
  (ReducibleQuery. connectable query options))

(defn reducible-query
  ([query]
   (reducible-query* conn/*connectable* query conn/*options*))

  ([connectable query]
   (reducible-query* connectable query nil))

  ([connectable query options]
   (reducible-query* connectable query options)))

(defn- query* [connectable query options]
  (conn/with-connection [[conn {:keys [rf init include-queries-in-exceptions?]
                                :or   {include-queries-in-exceptions? true}
                                :as   options}] connectable options]
    (assert rf)
    (assert init)
    (let [reducible (reducible-query connectable query options)]
      (reduce rf init reducible))))

(defn query
  ([a-query]                     (query* conn/*connectable* a-query conn/*options*))
  ([connectable a-query]         (query* connectable        a-query nil))
  ([connectable a-query options] (query* connectable        a-query options)))

(defn query-one
  ([query]
   (query-one conn/*connectable* query conn/*options*))

  ([connectable query]
   (query-one connectable query nil))

  ([connectable query options]
   (conn/with-connection [[conn {:keys [rf], :as options}] connectable options]
     (assert rf)
     (transduce
      (take 1)
      (completing rf first)
      nil
      (reducible-query connectable query options)))))

;; TODO -- execute
#_(defn execute! [connectable query options]
    (conn/with-connection [[conn options] connectable options]
      (let [sql-params (compile-query)])
      ))
