(ns fourcan.db.jdbc
  (:require [fourcan
             [compile :as compile]
             [debug :as debug]
             [hierarchy :as hierarchy]
             [types :as types]]
            [methodical.core :as m])
  (:import [java.sql Connection PreparedStatement ResultSet Statement Types]
           javax.sql.DataSource))

(m/defmulti data-source
  ;; FIXME — Methodical shouldn't override existing tags on multifn vars
  {:arglists '([model]), :tag `DataSource}
  keyword
  :hierarchy #'hierarchy/hierarchy)

(def ^:dynamic ^Connection *connection* nil)

(defn connection
  (^Connection []
   (connection nil))

  (^Connection [model]
   (or *connection*
       (.getConnection (data-source model)))))

;; TODO - this should use a multi-default dispatcher
(m/defmulti sql-value
  {:arglists '([model obj])}
  (fn [model obj]
    [(keyword model) (class obj)])
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod sql-value :default
  [_ obj]
  obj)

(m/defmethod sql-value :around :default
  [model obj]
  (let [result (next-method model obj)]
    (when debug/*debug*
      (printf "(sql-value %s ^%s %s) -> ^%s %s\n"
              (pr-str model) (.getName (class obj)) (pr-str obj)
              (.getName (class result)) (pr-str result)))
    result))

(m/defmulti set-parameter
  {:arglists '([model prepared-statement i obj])}
  (fn [model _ _ obj]
    [(keyword model) (class obj)])
  :hierarchy #'hierarchy/hierarchy)

;; TODO - debugging `:before` method

(m/defmethod set-parameter :around :default
  [model stmt i obj]
  (when debug/*debug*
    (printf "(set-parameter %s stmt %d ^%s %s)\n" (pr-str model) i (.getName (class obj)) (pr-str obj)))
  (next-method model stmt i obj))

(m/defmethod set-parameter :default
  [model ^PreparedStatement stmt ^Integer i obj]
  (.setObject stmt i (sql-value model obj)))

(m/defmulti set-parameters
  {:arglists '([model prepared-statement objs])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod ^PreparedStatement set-parameters :default
  [model stmt objs]
  (reduce
   (fn [i obj]
     (set-parameter model stmt i obj)
     (inc i))
   1
   objs)
  stmt)

;; TODO - I think compilation should happen in the db namespace, not here!
(defn prepare-statement
  ^PreparedStatement [model ^Connection conn form]
  (let [[^String sql & args] (compile/compile-honeysql model form)
        stmt                 (.prepareStatement conn sql Statement/RETURN_GENERATED_KEYS)]
    (if-not (seq args)
      stmt
      (try
        (set-parameters model stmt args)
        (catch Throwable e
          (.close stmt)
          (throw e))))))

;; TODO - these should be MultiFns
(defn read-value [model ^ResultSet rset ^Integer i ^Types col-type _]
  (.getObject rset i))

;; TODO - this should use a Methodical multi-default dispatcher (add)
(m/defmulti after-read
  {:arglists '([x])}
  (fn [model v] [(keyword model) (class v)])
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod after-read :default
  [_ v]
  v)

(defn read-row [model rset cols options]
  (reduce
   (fn [acc [i _ col-type]]
     (let [v (after-read model (read-value model rset i col-type options))]
       (conj acc v)))
   []
   cols))

(defn read-all-vectors [model ^ResultSet rset cols {:keys [row-xform]
                                                    :or   {row-xform identity}
                                                    :as   options}]
  (loop [acc []]
    (if-not (.next rset)
      acc
      (let [row (row-xform (read-row model rset cols options))]
        (recur (conj acc row))))))

(defn read-results [model ^ResultSet rset {:keys [results-fn]
                                           :or   {results-fn read-all-vectors}
                                           :as   options}]
  (let [rsmeta (.getMetaData rset)
        cols   (vec (for [^long i (range 1 (inc (.getColumnCount rsmeta)))]
                      [i (.getColumnName rsmeta i) (.getColumnType rsmeta i)]))]
    (results-fn model rset cols options)))

(m/defmulti query
  {:arglists '([model form options])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod query :default
  [model form options]
  (with-open [conn (connection model)
              stmt (prepare-statement model conn form)
              rset (.executeQuery stmt)]
    (read-results model rset options)))

(m/defmulti execute!*
  {:arglists '([model form])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

(m/defmethod execute!* :default
  [model form]
  (with-open [conn (connection model)
              stmt (prepare-statement model conn form)]
    (let [result (.executeUpdate stmt)]
      (printf "Execute successful, %d rows affected\n" result))))

(defn execute!
  ([hsql-form]
   (execute!* nil hsql-form))

  ([model hsql-form]
   (execute!* model hsql-form)))
