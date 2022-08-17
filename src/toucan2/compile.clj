(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([model query f])}
  u/dispatch-on-first-two-args)

(m/defmethod do-with-compiled-query :around :default
  [model query f]
  (u/println-debug [(list 'compile model query)])
  (next-method model query f))

(defmacro with-compiled-query [[query-binding [model query]] & body]
  `(do-with-compiled-query
    ~model
    ~query
    (^:once fn* [~query-binding] ~@body)))

(m/defmethod do-with-compiled-query [:default String]
  [_model sql f]
  (f [sql]))

(m/defmethod do-with-compiled-query [:default clojure.lang.Sequential]
  [_model sql-args f]
  (f sql-args))

;;;; HoneySQL options

(def global-honeysql-options
  (atom nil))

(def ^:dynamic *honeysql-options* nil)

;;; TODO -- should there be a model-specific HoneySQL options method as well? Or can model just bind
;;; [[*honeysql-options*]] inside of [[toucan2.model/with-model]] if they need to do something special? I'm leaning
;;; towards the latter.

(m/defmethod do-with-compiled-query [:default clojure.lang.IPersistentMap]
  [model honeysql f]
  (let [options (merge @global-honeysql-options
                       *honeysql-options*)]
    (u/println-debug ["Compiling Honey SQL with options %s" options])
    (let [sql-args (try
                     (hsql/format honeysql options)
                     (catch Throwable e
                       (throw (ex-info (format "Error building Honey SQL query: %s" (ex-message e))
                                       {:model    model
                                        :honeysql honeysql
                                        :options  options}
                                       e))))]
      (do-with-compiled-query model sql-args f))))
