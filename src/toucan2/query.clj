(ns toucan2.query
  (:require
   [methodical.core :as m]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rset]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.util :as u]))

(m/defmulti reduce-compiled-query
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-keyword-or-type-2)

(def ^:dynamic *jdbc-options*
  {:builder-fn jdbc.rset/as-unqualified-kebab-maps})

(m/defmethod reduce-compiled-query [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (reduce
   rf
   init
   (jdbc/plan conn sql-args  *jdbc-options*)))

(defrecord ReducibleQuery [connectable compileable]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (conn/with-connection [conn connectable]
      (compile/with-compiled-query [query [conn compileable]]
        (reduce-compiled-query conn query rf init)))))

(defn reducible-query [connectable compileable]
  (->ReducibleQuery connectable compileable))

#_(defn query [connectable compileable]
  (transduce
   (map (fn [row]
          (into {} row)))
   conj
   []
   (reducible-query connectable compileable)))
