(ns toucan2.query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.jdbc.query :as t2.jdbc.query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(m/defmulti reduce-query
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-first-two-args)

(m/defmethod reduce-query [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (t2.jdbc.query/reduce-jdbc-query conn sql-args rf init))

(defrecord ReducibleQuery [connectable query]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (conn/with-connection [conn connectable]
      (compile/with-compiled-query [query [conn query]]
        (reduce-query conn query rf init))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable query)))

(defn reducible-query [connectable query]
  (->ReducibleQuery connectable query))

(defn query [connectable query]
  (realize/realize (reducible-query connectable query)))

;; TODO -- query-as ?
