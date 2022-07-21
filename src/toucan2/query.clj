(ns toucan2.query
  (:require
   [methodical.core :as m]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rset]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.util :as u]))

(m/defmulti reduce-compiled-query
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-keyword-or-type-2)

(def global-jdbc-options
  (atom {:builder-fn jdbc.rset/as-unqualified-kebab-maps}))

(def ^:dynamic *jdbc-options* nil)

(m/defmethod reduce-compiled-query [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (reduce
   rf
   init
   (jdbc/plan conn sql-args (merge @global-jdbc-options *jdbc-options*))))

(defrecord ReducibleQuery [connectable query]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (conn/with-connection [conn connectable]
      (compile/with-compiled-query [query [conn query]]
        (reduce-compiled-query conn query rf init))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable query)))

(defn reducible-query [connectable query]
  (->ReducibleQuery connectable query))

#_(defn query [connectable query]
  (transduce
   (map (fn [row]
          (into {} row)))
   conj
   []
   (reducible-query connectable query)))
