(ns toucan2.compile
  (:require [methodical.core :as m]
            [honey.sql :as hsql]
            [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([connection compileable options f])}
  u/dispatch-on-keyword-or-type-2)

(defmacro with-compiled-query [[query-binding [connection compileable options]] & body]
  `(do-with-compiled-query
    ~connection
    ~compileable
    ~options
    (^:once fn* [~query-binding] ~@body)))

(m/defmethod do-with-compiled-query [java.sql.Connection String]
  [_conn sql _options f]
  (f [sql]))

(m/defmethod do-with-compiled-query [java.sql.Connection clojure.lang.Sequential]
  [_conn sql-args _options f]
  (f sql-args))

(m/defmethod do-with-compiled-query [java.sql.Connection clojure.lang.IPersistentMap]
  [conn honeysql options f]
  (let [sql-args (hsql/format honeysql (:honeysql options))]
    (do-with-compiled-query conn sql-args options f)))
