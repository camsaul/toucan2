(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.log :as log]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]))

(m/defmulti compile*
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

;; TODO -- not sure if `include-queries-in-exceptions?` should be something that goes in the options map or its own
;; dynamic variable.

(m/defmethod compile* :around :default
  [connectable tableable query {:keys [include-queries-in-exceptions?]
                                :or   {include-queries-in-exceptions? true}
                                :as   options}]
  (log/with-trace ["Compile query with table %s and options %s" tableable options]
    (log/trace (u/pprint-to-str query))
    (try
      (let [sql-params (next-method connectable tableable query options)]
        (assert (and (sequential? sql-params) (string? (first sql-params)))
                (str "compile* should return [sql & params], got " (pr-str sql-params)))
        sql-params)
      (catch Throwable e
        (throw (ex-info (format "Error compiling query: %s" (ex-message e))
                        (if include-queries-in-exceptions?
                          {:query query}
                          {})
                        e))))))

(m/defmethod compile* :default
  [_ tableable query options]
  (throw (ex-info (format "Don't know how to compile %s" (pr-str query))
                  {:tableable tableable
                   :query     query
                   :options   options})))

(m/defmethod compile* [:default :default String]
  [_ _ sql _]
  [sql])

;; [sql & params] or [honeysql & params] vector
(m/defmethod compile* [:default :default clojure.lang.Sequential]
  [connectable tableable [query & args] options]
  (into (compile* connectable tableable query options) args))

;; TODO -- I'm not 100% sure what the best way to pass conn/options information to HoneySQL stuff like
;; TableIdentifier is. Should we just use whatever is in place when we compile the HoneySQL form? Should a Table
;; identifier get its own options?
(def ^:private ^:dynamic *compile-connectable* nil)
(def ^:private ^:dynamic *compile-options* nil)

(m/defmethod compile* [:default :default clojure.lang.IPersistentMap]
  [connectable _ honeysql-form {options :honeysql}]
  (binding [*compile-connectable* connectable
            *compile-options*     options]
    (apply hsql/format honeysql-form (mapcat identity options))))

(defn compile
  ([queryable]
   (compile conn.current/*current-connectable* nil queryable nil))

  ([tableable queryable]
   (compile conn.current/*current-connectable* tableable queryable nil))

  ([connectable tableable queryable]
   (compile connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [options (u/recursive-merge (conn/default-options connectable) options)
         query   (when queryable
                   (queryable/queryable connectable tableable queryable options))]
     (compile* connectable tableable query options))))

(p/defrecord+ TableIdentifier [tableable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list* (u/qualify-symbol-for-*ns* `table-identifier) tableable (when options [options])))

  hformat/ToSql
  (to-sql [_]
    (let [options (u/recursive-merge *compile-options* options)]
      ;; TODO -- `:row-builder-fn` is present in `:honeysql` options in this log statement, but it shouldn't be. FIXME
      (log/with-trace (format "Convert table identifier %s to table name with options %s"
                              (pr-str tableable)
                              (pr-str (:honeysql options)))
        (-> (tableable/table-name *compile-connectable* tableable options)
            (hsql/quote-identifier :style (get-in options [:honeysql :quoting])))))))

(defn table-identifier
  ([tableable]         (->TableIdentifier tableable nil))
  ([tableable options] (->TableIdentifier tableable options)))

(m/defmulti from*
  "Add a SQL `FROM` clause or equivalent to `query`."
  {:arglists '([connectable tableable query options])}
  u/dispatch-on-first-three-args)

(m/defmethod from* :around :default
  [connectable tableable query options]
  (try
    (log/with-trace ["Adding :from %s to %s" tableable (pr-str query)]
      (next-method connectable tableable query options))
    (catch Throwable e
      (throw (ex-info (format "Error adding FROM %s to %s" (pr-str tableable) (pr-str query))
                      {:tableable tableable, :query query, :options options}
                      e)))))

;; default impl is a no-op
(m/defmethod from* :default
  [_ tableable query _]
  (log/tracef (format "Query %s isn't a map, not adding FROM %s" (pr-str query) (pr-str tableable)))
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
  ([tableable queryable]
   (from conn.current/*current-connectable* tableable queryable))

  ([connectable tableable queryable]
   (from connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [options (u/recursive-merge (conn/default-options connectable) options)
         query   (when queryable
                   (queryable/queryable connectable tableable queryable options))]
     (from* connectable tableable query options))))
