(ns toucan2.jdbc.query
  (:require [clojure.pprint :as pprint]
            [methodical.core :as m]
            [potemkin :as p]
            [pretty.core :as pretty]
            [toucan2.compile :as compile]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.jdbc.statement :as stmt]
            [toucan2.log :as log]
            [toucan2.query :as query]
            [toucan2.realize :as realize]
            [toucan2.util :as u]))

(p/deftype+ ReducibleSQLQuery [connectable tableable sql-params options]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (try
      (conn/with-connection [conn connectable tableable options]
        (log/with-trace ["Executing query %s with options %s" sql-params (:next.jdbc options)]
          (with-open [stmt (stmt/prepare connectable tableable conn sql-params options)]
            (let [results (stmt/reducible-statement connectable tableable stmt options)]
              (log/with-trace ["Reducing results with rf %s and init %s" rf init]
                (query/*call-count-thunk*)
                (reduce rf init results))))))
      (catch Throwable e
        (throw (ex-info (ex-message e) {:sql-params sql-params} e)))))

  clojure.lang.IDeref
  (deref [this]
    (realize/realize this))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `->ReducibleSQLQuery) connectable tableable sql-params options)))

(doseq [method [print-method pprint/simple-dispatch]]
  (prefer-method method pretty.core.PrettyPrintable clojure.lang.IDeref))

(p/deftype+ ReducibleQuery [connectable tableable queryable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list (u/qualify-symbol-for-*ns* `->ReducibleQuery) connectable tableable queryable options))

  clojure.lang.IReduceInit
  (reduce [this rf init]
    (try
      (let [reducible-sql-query (compile/compile* connectable tableable this options)]
        (reduce rf init reducible-sql-query))
      (catch Throwable e
        (throw (ex-info (ex-message e) {:query queryable} e)))))

  ;; convenience: deref a ReducibleQuery to realize all results.
  clojure.lang.IDeref
  (deref [this]
    (realize/realize this)))

(m/defmethod query/reducible-query* [:toucan2/jdbc :default :default]
  [connectable tableable queryable options]
  (->ReducibleQuery connectable tableable queryable options))

(m/defmethod compile/compile* [:default :default ReducibleQuery]
  [connectable tableable ^ReducibleQuery query options]
  (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)
        sql-params            (query/compile connectable tableable (.queryable query) options)]
    (->ReducibleSQLQuery connectable tableable sql-params options)))
