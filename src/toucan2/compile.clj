(ns toucan2.compile
  (:refer-clojure :exclude [compile])
  (:require [honeysql.format :as hformat]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [potemkin :as p]
            [pretty.core :as pretty]
            [toucan2.connectable.current :as conn.current]
            [toucan2.log :as log]
            [toucan2.queryable :as queryable]
            [toucan2.util :as u]))

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
  [connectable tableable query options]
  (throw (ex-info (binding [*print-meta* true]
                    (format "Don't know how to compile %s.\n%s"
                            (pr-str query)
                            (u/suggest-dispatch-values connectable tableable query)))
                  {:tableable tableable
                   :query     query
                   :options   options})))

(m/defmethod compile* [:default :default String]
  [_ _ sql _]
  [sql])

;; [sql & params] or [honeysql & params] vector
(m/defmethod compile* [:default :default clojure.lang.Sequential]
  [connectable tableable [queryable & args] options]
  (let [query (queryable/queryable* connectable tableable queryable options)]
    (into (compile* connectable tableable query options) args)))

(defn compilable?
  "True if `queryable` is a valid `compilable`, i.e. if there's a valid implementation of `compile*` for it."
  [connectable tableable queryable]
  (let [dispatch-value (m/dispatch-value compile* connectable tableable queryable)]
    (not= (-> (m/effective-primary-method compile* dispatch-value) meta :dispatch-value)
          :default)))

#_(m/defmethod queryable/queryable* :after :default
  [connectable tableable query _]
  (assert (compilable? connectable tableable query)
          (format "queryable* should return something compileable; got %s" (binding [*print-meta* true]
                                                                             (pr-str query))))
  query)

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

;; TODO -- this should be part of the HoneySQL stuff

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
