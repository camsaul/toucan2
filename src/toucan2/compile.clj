(ns toucan2.compile
  (:require [methodical.core :as m]
            [toucan2.util :as u]))

(def do-with-compiled-query nil) ; NOCOMMIT

(m/defmulti do-with-compiled-query
  {:arglists '([connection compileable f])}
  u/dispatch-on-keyword-or-type-2)

(defmacro with-compiled-query [[query-binding [connection compileable]] & body]
  `(do-with-compiled-query
    ~connection
    ~compileable
    (^:once fn* [~query-binding] ~@body)))

(m/defmethod do-with-compiled-query [java.sql.Connection String]
  [_conn sql f]
  (f [sql]))

(m/defmethod do-with-compiled-query [java.sql.Connection clojure.lang.Sequential]
  [_conn sql-args f]
  (f sql-args))
