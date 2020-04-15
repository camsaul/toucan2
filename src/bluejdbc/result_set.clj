(ns bluejdbc.result-set
  (:refer-clojure :exclude [type])
  (:require [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [methodical.core :as m])
  (:import [java.sql ResultSet ResultSetMetaData]))

(u/define-enums type            ResultSet #"^TYPE_"    :namespace :result-set-type)
(u/define-enums concurrency     ResultSet #"^CONCUR_"  :namespace :result-set-concurrency)
(u/define-enums holdability     ResultSet #"_CURSORS_" :namespace :result-set-holdability, :name-transform identity)
(u/define-enums fetch-direction ResultSet #"FETCH_")

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmeta ^Integer i options])}
  (fn [rs ^ResultSetMetaData rsmeta ^Integer i _]
    (let [col-type (u/reverse-lookup types/type (.getColumnType rsmeta i))]
      (log/tracef "Column %d %s is of type %s" i (pr-str (.getColumnLabel rsmeta i)) col-type)
      [(class rs) col-type])))

(m/defmethod read-column-thunk :default
  [^ResultSet rs rsmeta ^Integer i options]
  (let [rs-class-default-method (m/effective-method read-column-thunk (class rs))]
    (if-not (= rs-class-default-method (m/default-effective-method read-column-thunk))
      (do
        (log/tracef "Fetching values in column %d with %s default read-column-thunk method" i (.getCanonicalName (class rs)))
        (rs-class-default-method rs rsmeta i options))
      (do
        (log/tracef "Fetching values in column %d with (.getObject rs %d)" i i)
        (fn []
          (.getObject rs i))))))

(defn- get-object-of-class-thunk [^ResultSet rs, ^Integer i, ^Class klass]
  (log/tracef "Fetching values in column %d with (.getObject rs %d %s)" i i (.getCanonicalName klass))
  (fn []
    (.getObject rs i klass)))

(m/defmethod read-column-thunk [ResultSet :type/timestamp]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDateTime))

(m/defmethod read-column-thunk [ResultSet :type/timestamp-with-timezone]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetDateTime))

(m/defmethod read-column-thunk [ResultSet :type/date]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalDate))

(m/defmethod read-column-thunk [ResultSet :type/time]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.LocalTime))

(m/defmethod read-column-thunk [ResultSet :type/time-with-timezone]
  [rs _ i _]
  (get-object-of-class-thunk rs i java.time.OffsetTime))

(defn index-range [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn row-thunk
  "Returns a thunk that can be called repeatedly to get the next row in the result set, using appropriate methods to
  fetch each value in the row. Returns `nil` when the result set has no more rows."
  [^ResultSet rs options]
  (let [rsmeta (.getMetaData rs)
        fns    (for [i (index-range rsmeta)]
                 (read-column-thunk rs rsmeta (int i) options))]
    (let [thunk (apply juxt fns)]
      (fn row-thunk* []
        (when (.next rs)
          (thunk))))))

(defn column-names [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getColumnLabel rsmeta i))))))

(defn namespaced-column-names [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getTableName rsmeta i) (.getColumnLabel rsmeta i))))))

(defn maps-xform* [rs column-names]
  (fn [rf]
    (fn
      ([]
       (rf))

      ([acc]
       (rf acc))

      ([acc row]
       (rf acc (zipmap column-names row))))))

(defn maps-xform [rs]
  (maps-xform* rs (column-names rs)))

(defn namespaced-maps-xform [rs]
  (maps-xform* rs (namespaced-column-names rs)))

(defn reducible-results
  ([rs]
   (reducible-results rs nil))

  ([rs {xform :results/xform, :or {xform maps-xform} :as options}]
   (let [xform (if xform
                 (xform rs)
                 identity)
         thunk (row-thunk rs options)]
     (reify
       clojure.lang.IReduceInit
       (reduce [_ rf init]
         (let [rf (xform rf)]
           (loop [acc init]
             (if (reduced? acc)
               @acc
               (if-let [row (thunk)]
                 (recur (rf acc row))
                 (do
                   (log/trace "All rows consumed.")
                   acc))))))

       clojure.lang.IReduce
       (reduce [this rf]
         (reduce rf [] this))))))
