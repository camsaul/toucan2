(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [bluejdbc.connectable :as conn]
            [honeysql.core :as hsql]
            [methodical.core :as m]))

(m/defmulti compile*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

(m/defmethod compile* :around :default
  [connectable query {:keys [include-queries-in-exceptions?]
                      :or   {include-queries-in-exceptions? true}
                      :as   options}]
  (log/tracef "Compile query with options %s\n^%s %s"
              (u/pprint-to-str options)
              (some-> query class .getCanonicalName)
              (u/pprint-to-str query))
  (try
    (let [sql-params (next-method connectable query options)]
      (log/tracef "-> %s" (u/pprint-to-str sql-params))
      (assert (and (sequential? sql-params) (string? (first sql-params)))
              (str "compile* should return [sql & params], got " (pr-str sql-params)))
      sql-params)
    (catch Throwable e
      (throw (ex-info "Error compiling query"
                      (if include-queries-in-exceptions?
                        {:query query}
                        {})
                      e)))))

(m/defmethod compile* [:default String]
  [_ sql _]
  [sql])

;; [sql & params] or [honeysql & params] vector
(m/defmethod compile* [:default clojure.lang.Sequential]
  [connectable [query & args] options]
  (into (compile* connectable query options) args))

(m/defmethod compile* [:default clojure.lang.IPersistentMap]
  [_ honeysql-form {options :honeysql}]
  (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str options))
  (let [sql-params (apply hsql/format honeysql-form (mapcat identity options))]
    (log/tracef "->\n%s" (u/pprint-to-str sql-params))
    sql-params))

(defn compile
  ([query]                     (compile* conn/*connectable* query conn/*options*))
  ([connectable query]         (compile* connectable        query nil))
  ([connectable query options] (compile* connectable        query options)))
