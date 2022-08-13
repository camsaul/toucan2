(ns toucan2.compile
  (:require
   [honey.sql :as hsql]
   [honey.sql.helpers :as hsql.helpers]
   [methodical.core :as m]
   [toucan2.model :as model]
   [toucan2.util :as u]))

(m/defmulti do-with-compiled-query
  {:arglists '([queryable f])}
  u/dispatch-on-first-arg)

(m/defmethod do-with-compiled-query :around :default
  [queryable f]
  (u/println-debug (format "Compiling query ^%s %s" (some-> queryable class .getCanonicalName) (pr-str queryable)))
  (next-method queryable (fn [compiled-query]
                           (binding [u/*debug-indent-level* (inc u/*debug-indent-level*)]
                             (u/print-debug-result (pr-str compiled-query)))
                           (f compiled-query))))

(defmacro with-compiled-query [[query-binding queryable] & body]
  `(do-with-compiled-query
    ~queryable
    (^:once fn* [~query-binding] ~@body)))

(m/defmethod do-with-compiled-query String
  [sql f]
  (f [sql]))

(m/defmethod do-with-compiled-query clojure.lang.Sequential
  [sql-args f]
  (f sql-args))

;;;; HoneySQL options

(def global-honeysql-options
  (atom nil))

(def ^:dynamic *honeysql-options* nil)

(m/defmethod do-with-compiled-query clojure.lang.IPersistentMap
  [honeysql f]
  (let [sql-args (hsql/format honeysql (merge @global-honeysql-options
                                              *honeysql-options*))]
    (do-with-compiled-query sql-args f)))

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

(m/defmethod apply-condition [:default clojure.lang.IPersistentMap :toucan/pk]
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
