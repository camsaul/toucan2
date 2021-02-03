(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [methodical.core :as m])
  (:import java.sql.Connection))

(m/defmulti compile*
  {:arglists '([conn table query options])}
  u/dispatch-on-first-three-args)

(defn compile
  ([query]                    (compile* :default    nil              query nil))
  ([query options]            (compile* :default    (:table options) query options))
  ([conn query options]       (compile* conn (:table options) query options))
  ([conn table query options] (compile* conn table            query options)))

(m/defmethod compile* :around :default
  [conn table query options]
  (log/tracef "Compile query with options %s\n^%s %s"
              (u/pprint-to-str options)
              (some-> query class .getCanonicalName)
              (u/pprint-to-str query))
  (let [sql-args (next-method conn table query options)]
    (log/tracef "-> %s" (u/pprint-to-str sql-args))
    (assert (and (sequential? sql-args) (string? (first sql-args)))
            (str "compile* should return [sql & args], got " (pr-str sql-args)))
    sql-args))

(m/defmethod compile* [:default :default String]
  [_ _ sql _]
  [sql])

;; [sql & args] or [honeysql & args] vector
(m/defmethod compile* [:default :default clojure.lang.Sequential]
  [conn table [query & args] options]
  (into (compile* conn table query options) args))

(defn format-honeysql
  "Compile a HoneySQL form into `[sql & args]`."
  [honeysql-form options]
  (let [honeysql-options (into {} (for [[k v] options
                                        :when (= (namespace k) "honeysql")]
                                    [(keyword (name k)) v]))]
    (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str honeysql-options))
    (let [sql-args (apply hsql/format honeysql-form (mapcat identity honeysql-options))]
      (log/tracef "->\n%s" (u/pprint-to-str sql-args))
      sql-args)))

(defn- quoting-strategy [^Connection conn]
  (case (.. conn getMetaData getIdentifierQuoteString)
    "\"" :ansi
    "`"  :mysql
    nil))

(m/defmethod compile* [:default :default clojure.lang.IPersistentMap]
  [conn _ honeysql-form options]
  (let [options (cond-> options
                  (and conn (not (:honeysql/quoting options)))
                  (assoc :honeysql/quoting (quoting-strategy conn)))]
    (format-honeysql honeysql-form options)))

(defn quote-identifier [identifier options]
  (hsql/quote-identifier identifier :style (or (:honeysql/quoting options)
                                               :ansi)))
