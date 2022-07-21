(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([connection query options f])}
  u/dispatch-on-keyword-or-type-2)

(m/defmethod do-with-compiled-query :around :default
  [connection query options f]
  (if-not u/*debug*
    (next-method connection query options f)
    (do
      (u/println-debug (format "Compile query for %s connection %s"
                               (some-> connection class .getCanonicalName)
                               (pr-str query)))
      (binding [u/*debug-indent-level* (inc u/*debug-indent-level*)]
        (next-method connection query options (fn [compiled-query]
                                                (binding [u/*debug-indent-level* (dec u/*debug-indent-level*)]
                                                  (u/println-debug '=> (pr-str compiled-query)))
                                                (f compiled-query)))))))

(defmacro with-compiled-query [[query-binding [connection query options]] & body]
  `(do-with-compiled-query
    ~connection
    ~query
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
