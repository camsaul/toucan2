(ns bluejdbc.statement
  "Protocols, methods, functions, macros, etc. for working with `java.sql.PreparedStatement`s."
  (:require [bluejdbc.log :as log]
            [java-time :as t]
            [methodical.core :as m])
  (:import [java.sql Connection PreparedStatement ResultSet Statement Types]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]))

;;;; Parameters

(defn set-object!
  "Set `PreparedStatement` parameter at index `i` to `object` by calling `.setObject`, optionally with a
  `target-sql-type` (which may be either the raw Java enum integer, e.g. `java.sql.Types/INTEGER`, or the keyword name
  of it, e.g. `:integer`.)"
  ([^PreparedStatement stmt ^Integer i object]
   (log/tracef "(set-object! stmt %d ^%s %s)" i (some-> object class .getCanonicalName) (pr-str object))
   (.setObject stmt i object))

  ([^PreparedStatement stmt ^Integer i object ^Integer target-sql-type]
   (log/tracef "(set-object! stmt %d ^%s %s %s)" i
               (some-> object class .getCanonicalName) (pr-str object)
               target-sql-type)
   (.setObject stmt i object target-sql-type)))

(m/defmulti set-parameter!
  "Set `PreparedStatement` parameter at index `i` to `object`. Dispatches on `:connection/type`, if present, in
  `options`; and by the class of `Object`. The default implementation calls `set-object!`."
  {:arglists '([^PreparedStatement stmt ^Integer i object options])}
  (fn [_ _ object options]
    [(:connection/type options) (class object)]))

(m/defmethod set-parameter! :default
  [stmt i object options]
  (set-object! stmt i object))

(m/defmethod set-parameter! [:default LocalDate]
  [stmt i t _]
  (set-object! stmt i t Types/DATE))

(m/defmethod set-parameter! [:default LocalTime]
  [stmt i t _]
  (set-object! stmt i t Types/TIME))

(m/defmethod set-parameter! [:default LocalDateTime]
  [stmt i t _]
  (set-object! stmt i t Types/TIMESTAMP))

(m/defmethod set-parameter! [:default OffsetTime]
  [stmt i t _]
  (set-object! stmt i t Types/TIME_WITH_TIMEZONE))

(m/defmethod set-parameter! [:default OffsetDateTime]
  [stmt i t _]
  (set-object! stmt i t Types/TIMESTAMP_WITH_TIMEZONE))

(m/defmethod set-parameter! [:default ZonedDateTime]
  [stmt i t _]
  (set-object! stmt i t Types/TIMESTAMP_WITH_TIMEZONE))

(m/defmethod set-parameter! [:default Instant]
  [driver stmt i t]
  (set-parameter! driver stmt i (t/offset-date-time t (t/zone-offset 0))))

(defn set-parameters!
  "Set parameters for the prepared statement by calling `set-parameter!` for each parameter. Returns `stmt`."
  ^PreparedStatement [stmt params options]
  (dorun
   (map-indexed
    (fn [i param]
      (log/tracef "Set param %d -> %s" (inc i) (pr-str param))
      (set-parameter! stmt (inc i) param options))
    params))
  stmt)

(defn prepare!
  "Create a new `PreparedStatement` from `query`. `query` is one of:"
  (^PreparedStatement [conn sql-args]
   (prepare! conn sql-args nil))

  (^PreparedStatement [^Connection conn
                       sql-params
                       {^Integer rs-type      :result-set/type
                        ^Integer concurrency  :result-set/concurrency
                        ^Integer holdability  :result-set/holdability
                        return-generated-keys :statement/return-generated-keys
                        :or                   {rs-type     ResultSet/TYPE_FORWARD_ONLY
                                               concurrency ResultSet/CONCUR_READ_ONLY
                                               holdability (.getHoldability conn)}
                        :as                   options}]
   {:pre [(instance? java.sql.Connection conn)]}
   (let [[^String s & params]
         (if (string? sql-params)
           [sql-params]
           sql-params)

         stmt
         (cond
           (and return-generated-keys
                (sequential? return-generated-keys))
           (do
             (log/trace (pr-str (list '.prepareStatement 'conn s (mapv name return-generated-keys))))
             (.prepareStatement conn
                                s
                                ^"[Ljava.lang.String;" (into-array String (map name return-generated-keys))))

           return-generated-keys
           (do
             (log/trace (pr-str (list '.prepareStatement 'conn s 'Statement/RETURN_GENERATED_KEYS)))
             (.prepareStatement conn
                                s
                                Statement/RETURN_GENERATED_KEYS))

           :else
           (do
             (log/trace (pr-str (list '.prepareStatement 'conn s
                                      rs-type
                                      concurrency
                                      holdability)))
             (.prepareStatement conn
                                s
                                rs-type
                                concurrency
                                holdability)))]
     (set-parameters! stmt params options))))
