(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([model compiled-query f])}
  u/dispatch-on-first-two-args)

(defmacro with-compiled-query [[query-binding [model query]] & body]
  `(*with-compiled-query-fn*
    ~model
    ~query
    (^:once fn* [~query-binding] ~@body)))

(def ^:dynamic ^{:arglists '([model compiled-query f])} *with-compiled-query-fn*
  "The function that should be invoked by [[with-compiled-query]]. By default, the multimethod [[do-with-compiled-query]],
  but if you need to do some sort of crazy mocking you can swap this out with something else.
  See [[toucan2.tools.compile/build]] for an example usage."
  #'do-with-compiled-query)

(m/defmethod do-with-compiled-query :around :default
  [model uncompiled f]
  (let [f* (^:once fn* [compiled]
            (when (and u/*debug*
                       (not= uncompiled compiled))
              (u/with-debug-result ["compile %s query %s" model uncompiled]
                compiled))
            (u/try-with-error-context (when (not= uncompiled compiled)
                                        ["with compiled query" {::compiled compiled}])
              (f compiled)))]
    (next-method model uncompiled f*)))

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
        _        (u/println-debug ["Compiling Honey SQL 2 with options %s" options])
        sql-args (u/try-with-error-context ["compile Honey SQL query" {::honeysql honeysql, ::options options}]
                   (hsql/format honeysql options))]
    (do-with-compiled-query model sql-args f)))
