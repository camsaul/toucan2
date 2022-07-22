(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([connection query f])}
  u/dispatch-on-first-two-args)

(m/defmethod do-with-compiled-query :around :default
  [connection query f]
  (if-not u/*debug*
    (next-method connection query f)
    (do
      (u/println-debug (format "Compile query for %s connection %s"
                               (some-> connection class .getCanonicalName)
                               (pr-str query)))
      (binding [u/*debug-indent-level* (inc u/*debug-indent-level*)]
        (next-method connection query (fn [compiled-query]
                                        (binding [u/*debug-indent-level* (dec u/*debug-indent-level*)]
                                          (u/print-debug-result (pr-str compiled-query))
                                          (f compiled-query))))))))

(defmacro with-compiled-query [[query-binding [connection query]] & body]
  `(do-with-compiled-query
    ~connection
    ~query
    (^:once fn* [~query-binding] ~@body)))

(m/defmethod do-with-compiled-query [:default String]
  [_conn sql f]
  (f [sql]))

(m/defmethod do-with-compiled-query [:default clojure.lang.Sequential]
  [_conn sql-args f]
  (f sql-args))

;;;; HoneySQL options

(def global-honeysql-options
  (atom nil))

(def ^:dynamic *honeysql-options* nil)

(m/defmethod do-with-compiled-query [:default clojure.lang.IPersistentMap]
  [conn honeysql f]
  (let [sql-args (hsql/format honeysql (merge @global-honeysql-options
                                              *honeysql-options*))]
    (do-with-compiled-query conn sql-args f)))
