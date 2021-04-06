(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [methodical.core :as m]))

(m/defmulti compile*
  {:arglists '([connectable table query options])}
  u/dispatch-on-first-three-args)

(defn compile
  ([query]                           (compile  :current    nil   query nil))
  ([query options]                   (compile  :current    nil   query options))
  ([table query options]             (compile  nil         table query options))
  ([connectable table query options] (compile* connectable table query (merge (conn/default-options connectable)
                                                                              options))))

(m/defmethod compile* :around :current
  [connectable table query options]
  (log/tracef "Compile query with options %s\n^%s %s"
              (u/pprint-to-str options)
              (some-> query class .getCanonicalName)
              (u/pprint-to-str query))
  (let [sql-params (next-method connectable table query options)]
    (log/tracef "-> %s" (u/pprint-to-str sql-params))
    (assert (and (sequential? sql-params) (string? (first sql-params)))
            (str "compile* should return [sql & params], got " (pr-str sql-params)))
    sql-params))

(m/defmethod compile* [:default :default String]
  [_ _ sql _]
  [sql])

;; [sql & params] or [honeysql & params] vector
(m/defmethod compile* [:default :default clojure.lang.Sequential]
  [conn table [query & args] options]
  (into (compile* conn table query options) args))

(m/defmethod compile* [:default :default clojure.lang.IPersistentMap]
  [_ _ honeysql-form {options :honeysql}]
  (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str options))
  (let [sql-params (apply hsql/format honeysql-form (mapcat identity options))]
    (log/tracef "->\n%s" (u/pprint-to-str sql-params))
    sql-params))

#_(defn- quoting-strategy [^Connection conn]
  (case (.. conn getMetaData getIdentifierQuoteString)
    "\"" :ansi
    "`"  :mysql
    nil))

(defn quote-identifier
  ([identifier]
   (quote-identifier :current identifier nil))

  ([identifier options]
   (quote-identifier :current identifier options))

  ([connectable identifier options]
   (let [options (merge (conn/default-options connectable) options)]
     (hsql/raw (hsql/quote-identifier identifier :style (get-in options [:honeysql :quoting] :ansi))))))
