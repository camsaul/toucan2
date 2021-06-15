(ns toucan2.compile
  (:refer-clojure :exclude [compile])
  (:require [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
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
     (assert (some? query) "Query cannot be nil")
     (when (seqable? query)
       (assert (seq query) (format "Query cannot be empty. Got: %s" (binding [*print-meta* true] (pr-str query)))))
     (compile* connectable tableable query options))))
