(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.model :as model]
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

;;;; Applying conditions

(m/defmulti apply-condition
  {:arglists '([model query k v])}
  u/dispatch-on-first-three-args)

(defn condition->honeysql-where-clause [k v]
  (if (sequential? v)
    (vec (list* (first v) k (rest v)))
    [:= k v]))

(m/defmethod apply-condition [:default clojure.lang.IPersistentMap :default]
  [_model honeysql k v]
  (update honeysql :where (fn [existing-where]
                            (:where (hsql.helpers/where existing-where
                                                        (condition->honeysql-where-clause k v))))))

(m/defmethod apply-condition [:default clojure.lang.IPersistentMap :toucan2/pk]
  [model honeysql _k v]
  (let [pk-columns (model/primary-keys model)
        v          (if (sequential? v)
                     v
                     [v])]
    (assert (= (count pk-columns)
               (count v))
            (format "Expected %s primary key values for %s, got %d values %s"
                    (count pk-columns) (pr-str pk-columns)
                    (count v) (pr-str v)))
    (reduce
     (fn [honeysql [k v]]
       (apply-condition model honeysql k v))
     honeysql
     (zipmap pk-columns v))))

(defn apply-conditions [model query conditions]
  (reduce
   (fn [query [k v]]
     (apply-condition model query k v))
   query
   conditions))
