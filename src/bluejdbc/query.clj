(ns bluejdbc.query
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [next.jdbc :as next.jdbc]
            [potemkin :as p]))

;; TODO -- should this be off by default?
(def ^:dynamic *include-queries-in-exceptions?* true)

(p/defprotocol+ IReducibleQuery
  (options [this])
  (with-options [this new-options]))

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
              (let [message (or (:message (ex-data e)) (ex-message e))]
                (throw (ex-info (format "Error reducing results: %s" message)
                                {:rf rf, :init init, :message message}
                                e))))))
        (catch Throwable e
          (let [message (or (:message (ex-data e)) (ex-message e))]
            (throw (ex-info (format "Error executing query: %s" message)
                            (merge
                             {:options options
                              :message message}
                             (when *include-queries-in-exceptions?*
                               {:query queryable, :sql-params sql-params, :options options}))
                            e))))))))

(p/deftype+ ReducibleQuery [connectable tableable queryable options]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce-query connectable tableable queryable options rf init))

  IReducibleQuery
  (options [_]
    (u/recursive-merge (conn/default-options connectable) options))

  (with-options [_ new-options]
    (ReducibleQuery. connectable tableable queryable new-options)))

(defn all [reducible-query]
  (let [options (options reducible-query)]
    (transduce
     (:xform options identity)
     (:rf options u/default-rf)
     (:init options [])
     reducible-query)))

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

(defn compose-xform
  ([reducible-query new-xform]
   (let [opts (-> (options reducible-query)
                  (update :xform (fn [xform]
                                   (if xform
                                     (comp xform new-xform)
                                     new-xform))))]
     (with-options reducible-query opts)))

  ([reducible-query new-xform & more]
   (apply compose-xform (compose-xform reducible-query new-xform) more)))

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable tableable queryable]
                   [connectable tableable queryable options])}
  query
  (comp all reducible-query))

(def ^{:arglists '([queryable]
                   [connectable queryable]
                   [connectable tableable queryable]
                   [connectable tableable queryable options])}
  query-one
  (comp
   all
   (fn [query]
     (compose-xform query (take 1) #(completing % first)))
   reducible-query))

;; TODO -- execute
#_(defn execute! [connectable query options]
    (conn/with-connection [conn connectable options]
      (let [sql-params (compile-query)])
      ))
