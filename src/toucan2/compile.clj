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
  (u/println-debug ["compile %s query %s" model query])
  (try
    (next-method model query f)
    (catch Throwable e
      (throw (ex-info (.getMessage e)
                      {:model model, :uncompiled-query query}
                      e)))))

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

(defonce global-honeysql-options
  (atom nil))

;; TODO -- rename this to `*options*` or something, because even if we don't use Honey SQL 2 as a backend we might still
;; want to use them and include them in the error message.
(def ^:dynamic *honeysql-options* nil)

;;; TODO -- should there be a model-specific HoneySQL options method as well? Or can model just bind
;;; [[*honeysql-options*]] inside of [[toucan2.model/with-model]] if they need to do something special? I'm leaning
;;; towards the latter.

(m/defmethod do-with-compiled-query [:default clojure.lang.IPersistentMap]
  [model honeysql f]
  (let [options  (merge @global-honeysql-options
                        *honeysql-options*)
        sql-args (u/with-debug-result ["Compiling Honey SQL with options %s" options]
                   (try
                     (hsql/format honeysql options)
                     (catch Throwable e
                       (throw (ex-info (format "Error building query: %s" (ex-message e))
                                       {:model model, :honeysql honeysql, :options options}
                                       e)))))]
    (do-with-compiled-query model sql-args f)))
