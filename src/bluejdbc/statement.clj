(ns bluejdbc.statement
  "Protocols, methods, functions, macros, etc. for working with `java.sql.PreparedStatement`s."
  (:require [bluejdbc.options :as options]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [methodical.core :as m]
            [potemkin.types :as p.types])
  (:import [java.sql Connection PreparedStatement]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]))

(defn set-object
  ([^PreparedStatement stmt ^Integer index object]
   (log/tracef "(set-object stmt %d ^%s %s)" index (some-> object class .getCanonicalName) (pr-str object))
   (.setObject stmt index object))

  ([^PreparedStatement stmt ^Integer index object target-sql-type]
   (log/tracef "(set-object stmt %d ^%s %s %s)" index
               (some-> object class .getCanonicalName) (pr-str object)
               (u/reverse-lookup types/type target-sql-type))
   (.setObject stmt index object (types/type target-sql-type))))

(m/defmulti set-parameter
  {:arglists '([^PreparedStatement stmt ^Integer i object options])}
  (fn [stmt _ object _]
    [(class stmt) (class object)]))

(m/defmethod set-parameter :default
  [stmt i object options]
  ;; TODO -- use a custom Methodical dispatcher to accomplish this
  (let [class-default-method (m/effective-method set-parameter (class stmt))]
    (if-not (= class-default-method (m/default-effective-method set-parameter))
      (class-default-method stmt i object options)
      (set-object stmt i object))))

(m/defmethod set-parameter [PreparedStatement LocalDate]
  [stmt i t _]
  (set-object stmt i t :date))

(m/defmethod set-parameter [PreparedStatement LocalTime]
  [stmt i t _]
  (set-object stmt i t :time))

(m/defmethod set-parameter [PreparedStatement LocalDateTime]
  [stmt i t _]
  (set-object stmt i t :timestamp))

(m/defmethod set-parameter [PreparedStatement OffsetTime]
  [stmt i t _]
  (set-object stmt i t :time-with-timezone))

(m/defmethod set-parameter [PreparedStatement OffsetDateTime]
  [stmt i t _]
  (set-object stmt i t :timestamp-with-timezone))

(m/defmethod set-parameter [PreparedStatement ZonedDateTime]
  [stmt i t _]
  (set-object stmt i t :timestamp-with-timezone))

(m/defmethod set-parameter [PreparedStatement Instant]
  [driver stmt i t]
  (set-parameter driver stmt i (t/offset-date-time t (t/zone-offset 0))))

(defn set-parameters
  "Set parameters for the prepared statement by calling `set-parameter` for each parameter. Returns `stmt`."
  ^PreparedStatement [stmt params options]
  (dorun
   (map-indexed
    (fn [i param]
      (log/tracef "Set param %d -> %s" (inc i) (pr-str param))
      (set-parameter stmt (inc i) param options))
    params))
  stmt)

(p.types/defprotocol+ NewPreparedStatement
  (prepare ^java.sql.PreparedStatement [this ^Connection conn options]))

(defn format-honeysql [honeysql-form options]
  (let [honeysql-options (into {} (for [[k v] options
                                        :when (= (namespace k) "honeysql")]
                                    [(keyword (name k)) v]))]
    (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str honeysql-options))
    (let [sql-args (apply hsql/format honeysql-form (mapcat identity honeysql-options))]
      (log/tracef "->\n%s" (u/pprint-to-str sql-args))
      sql-args)))

(extend-protocol NewPreparedStatement
  ;; plain SQL string
  String
  (prepare [s ^Connection conn {rs-type     :result-set/type
                                concurrency :result-set/concurrency
                                holdability :result-set/holdability
                                :or         {rs-type     :forward-only
                                             concurrency :read-only
                                             holdability (.getHoldability conn)}
                                :as         options}]
    (let [stmt (.prepareStatement conn
                                  s
                                  (result-set/type rs-type)
                                  (result-set/concurrency concurrency)
                                  (result-set/holdability holdability))]
      (options/with-options stmt options)))

  ;; [sql & args] or [honeysql & args] vector
  clojure.lang.Sequential
  (prepare [[query & params] conn options]
    (-> (prepare query conn options)
        (set-parameters params options)))

  ;; HoneySQL map
  clojure.lang.IPersistentMap
  (prepare [honeysql-form conn options]
    (prepare (format-honeysql honeysql-form options) conn options)))

(defn prepared-statement
  (^PreparedStatement [conn query]
   (prepared-statement conn query nil))

  (^PreparedStatement [conn query options]
   (prepare query conn options)))
