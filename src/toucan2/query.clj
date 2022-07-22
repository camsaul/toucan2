(ns toucan2.query
  (:require
   [methodical.core :as m]
   [next.jdbc :as jdbc]
   [next.jdbc.result-set :as jdbc.rset]
   [pretty.core :as pretty]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.util :as u]
   [toucan2.realize :as realize]))

(m/defmulti reduce-compiled-query
  {:arglists '([connection compiled-query rf init])}
  u/dispatch-on-keyword-or-type-2)

(def global-jdbc-options
  ;; this default was chosen mostly to mimic Toucan 1 behavior
  ;; TODO -- consider whether maybe it's better to just return instances with model = `nil`
  (atom {:builder-fn jdbc.rset/as-unqualified-lower-maps}))

(def ^:dynamic *jdbc-options* nil)

(m/defmethod reduce-compiled-query [java.sql.Connection clojure.lang.Sequential]
  [conn sql-args rf init]
  (let [options (merge @global-jdbc-options *jdbc-options*)]
    (u/with-debug-result (format "Reducing query with options %s" (pr-str options))
      (reduce rf init (jdbc/plan conn sql-args options)))))

(defrecord ReducibleQuery [connectable query]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (conn/with-connection [conn connectable]
      (compile/with-compiled-query [query [conn query]]
        (reduce-compiled-query conn query rf init))))

  realize/Realize
  (realize [this]
    (into []
          (map (fn [row]
                 (jdbc.rset/datafiable-row row connectable nil)))
          this))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-query connectable query)))

(defn reducible-query [connectable query]
  (->ReducibleQuery connectable query))

(defn query [connectable query]
  (realize/realize (reducible-query connectable query)))

;; TODO -- query-as ?
