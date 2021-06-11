(ns bluejdbc.compile
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.log :as log]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [potemkin :as p]
            [pretty.core :as pretty]))

(m/defmulti compile*
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

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
        #_(assert (and (sequential? sql-params) (string? (first sql-params)))
                (str "compile* should return [sql & params], got " (pr-str sql-params)))
        sql-params)
      (catch Throwable e
        (throw (ex-info (format "Error compiling query: %s" (ex-message e))
                        (if include-queries-in-exceptions?
                          {:query query}
                          {})
                        e))))))

(m/defmethod compile* :default
  [queryable tableable query options]
  (throw (ex-info (format "Don't know how to compile %s. %s" query (u/suggest-dispatch-values queryable tableable query))
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
  (assert (not (contains? honeysql-form :next.jdbc))
          (format "Options should not be present in honeysql-form! Got: %s" (pr-str honeysql-form)))
  (binding [*compile-connectable* connectable
            *compile-options*     options]
    (apply hsql/format honeysql-form (mapcat identity options))))

(defn compile
  ([queryable]
   (compile (conn.current/current-connectable) nil queryable nil))

  ([tableable queryable]
   (compile (conn.current/current-connectable tableable) tableable queryable nil))

  ([connectable tableable queryable]
   (compile connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
         query                 (when queryable
                                 (queryable/queryable connectable tableable queryable options))]
     (compile* connectable tableable query options))))

;; TODO -- should `connectable` be an arg here too?
(p/defrecord+ TableIdentifier [tableable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list* (pretty/qualify-symbol-for-*ns* `table-identifier) tableable (when options [options])))

  hformat/ToSql
  (to-sql [_]
    (let [options (u/recursive-merge *compile-options* options)]
      (log/with-trace (format "Convert table identifier %s to table name with options %s"
                              (pr-str tableable)
                              (pr-str (:honeysql options)))
        (-> (tableable/table-name *compile-connectable* tableable options)
            (hsql/quote-identifier :style (get-in options [:honeysql :quoting])))))))

(defn table-identifier
  ([tableable]         (->TableIdentifier tableable nil))
  ([tableable options] (->TableIdentifier tableable options)))

(m/defmulti to-sql*
  {:arglists '([connectableᵈ tableableᵈ columnᵈ valueᵈᵗ options])}
  u/dispatch-on-first-four-args
  :combo (m.combo.threaded/threading-method-combination :fourth))

(m/defmethod to-sql* :around :default
  [connectable tableable column v options]
  (let [result (next-method connectable tableable column v options)]
    (if (sequential? result)
      (let [[s & params] result]
        (assert (string? s) (format "to-sql* should return either string or [string & parms], got %s" (pr-str result)))
        (doseq [param params]
          (hformat/add-anon-param param))
        s)
      result)))

(p/defrecord+ Value [connectable tableable column v options]
  pretty/PrettyPrintable
  (pretty [_]
    (list* (pretty/qualify-symbol-for-*ns* `value) connectable tableable column v (when options [options])))

  hformat/ToSql
  (to-sql [this]
    (log/with-trace ["Convert %s to SQL" this]
      (to-sql* connectable tableable column v options))))

(defn value
  ([connectable tableable column v]
   (value connectable tableable column v nil))

  ([connectable tableable column v options]
   (assert (keyword? column) (format "column should be a keyword, got %s" (pr-str column)))
   (when (seq options)
     (assert (map? options) (format "options should be a map, got %s" (pr-str options))))
   (->Value connectable tableable column v options)))

(defn maybe-wrap-value
  "If there's an applicable impl of `to-sql*` for connectable + tableable + column + (class v), wrap in `v` in a
  `Value`; if there's not one, return `v` as-is."
  ([connectable tableable column v]
   (maybe-wrap-value connectable tableable column v nil))

  ([connectable tableable column v options]
   (let [dv (m/dispatch-value to-sql* connectable tableable column v)]
     (if (m/effective-primary-method to-sql* dv)
       (log/with-trace ["Found to-sql* method impl for dispatch value %s; wrapping in Value" dv]
         (value connectable tableable column v options))
       v))))

(m/defmulti from*
  "Add a SQL `FROM` clause or equivalent to `query`."
  {:arglists '([connectableᵈ tableableᵈ queryᵈᵗ options])}
  u/dispatch-on-first-three-args)

(m/defmethod from* :around :default
  [connectable tableable query options]
  (try
    (log/with-trace ["Adding :from %s to" tableable]
      (log/trace (u/pprint-to-str query))
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
  (if (:from query)
    (do
      (log/tracef "Query already has :from; not adding one for %s" tableable)
      query)
    (assoc query :from [(table-identifier tableable (not-empty (select-keys options [:honeysql])))])))

(defn from
  "Add a `:from` clause for `tableable` to `queryable` e.g.

    (from :my-table {:select [:*]}) ; -> {:select [:*], :from [:my-table]}"
  ;; I considering arglists of [query tableable], [query tableable options], and [query connectable tableable options]
  ;; to facilitate threading, but I thought this would used in a threading context relatively rarely and it's not
  ;; worth the added cognitive load of having the arguments appear in a different order in this place and nowhere
  ;; else.
  ([tableable queryable]             (from nil         tableable queryable nil))
  ([connectable tableable queryable] (from connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
         query                 (when queryable
                                 (queryable/queryable connectable tableable queryable options))]
     (from* connectable tableable query options))))
