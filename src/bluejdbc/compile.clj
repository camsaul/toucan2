(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.log :as log]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]))

(m/defmulti compile*
  {:arglists '([connectable query options])}
  u/dispatch-on-first-two-args)

;; TODO -- not sure if `include-queries-in-exceptions?` should be something that goes in the options map or its own
;; dynamic variable.

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

;; TODO -- I'm not 100% sure what the best way to pass conn/options information to HoneySQL stuff like
;; TableIdentifier is. Should we just use whatever is in place when we compile the HoneySQL form? Should a Table
;; identifier get its own options?
(def ^:private ^:dynamic *compile-connectable* nil)
(def ^:private ^:dynamic *compile-options* nil)

(m/defmethod compile* [:default clojure.lang.IPersistentMap]
  [connectable honeysql-form {options :honeysql}]
  (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str options))
  (binding [*compile-connectable* connectable
            *compile-options*     options]
    (let [sql-params (apply hsql/format honeysql-form (mapcat identity options))]
      (log/tracef "->\n%s" (u/pprint-to-str sql-params))
      sql-params)))

(defn compile
  ([query]                     (compile* conn/*connectable* query (conn/default-options conn/*connectable*)))
  ([connectable query]         (compile* connectable        query (conn/default-options connectable)))
  ([connectable query options] (compile* connectable        query (u/recursive-merge (conn/default-options connectable) options))))

(p/defrecord+ TableIdentifier [tableable options]
  pretty/PrettyPrintable
  (pretty [_]
    (if options
      (list `table-identifier tableable options)
      (list `table-identifier tableable)))

  hformat/ToSql
  (to-sql [_]
    (let [options (u/recursive-merge *compile-options* options)]
      (-> (tableable/table-name *compile-connectable* tableable options)
          (hsql/quote-identifier :style (get-in options [:honeysql :quoting]))))))

(defn table-identifier
  ([tableable]         (->TableIdentifier tableable nil))
  ([tableable options] (->TableIdentifier tableable options)))

(m/defmulti from*
  "Add a SQL `FROM` clause or equivalent to `query`."
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

;; default method is a no-op
(m/defmethod from* :default
  [_ _ query _]
  query)

;; method for HoneySQL maps
(m/defmethod from* [:default :default clojure.lang.IPersistentMap]
  [_ tableable query options]
  (assoc query :from [(table-identifier tableable (not-empty (select-keys options [:honeysql])))]))

(defn from
  ;; I considering arglists of [query tableable], [query tableable options], and [query connectable tableable options]
  ;; to facilitate threading, but I thought this would used in a threading context relatively rarely and it's not
  ;; worth the added cognitive load of having the arguments appear in a different order in this place and nowhere
  ;; else.
  ([tableable query]                     (from* conn/*connectable* tableable query (conn/default-options conn/*connectable*)))
  ([connectable tableable query]         (from* connectable        tableable query (conn/default-options connectable)))
  ([connectable tableable query options] (from* connectable        tableable query (u/recursive-merge (conn/default-options connectable) options))))
