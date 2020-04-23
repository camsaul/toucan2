(ns bluejdbc.integrations.postgres
  (:require [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.util :as u]
            [clojure.string :as str]
            [clojure.tools.logging :as log]
            [java-time :as t]
            [methodical.core :as m]
            [second-date.core :as second-date])
  (:import [java.sql ResultSet ResultSetMetaData]))

;; Postgres doesn't support OffsetTime, so convert to a LocalTime in the system timezone
(m/defmethod stmt/set-parameter! [:postgresql java.time.OffsetTime]
  [stmt i t options]
  (let [local-time (t/local-time (t/with-offset-same-instant t (t/zone-offset)))]
    (stmt/set-parameter! stmt i local-time options)))

;; for some reason postgres `TIMESTAMP WITH TIME ZONE` columns still come back as `Type/TIMESTAMP`, which seems like a
;; bug with the JDBC driver?
;;
;; We can look at the actual column type name to distinguish between the two
(m/defmethod rs/read-column-thunk [:postgresql :type/timestamp]
  [^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i _]
  (let [^Class klass (if (= (str/lower-case (.getColumnTypeName rsmeta i)) "timestamptz")
                       java.time.OffsetDateTime
                       java.time.LocalDateTime)]
    (fn []
      (.getObject rs i klass))))

;; Sometimes Postgres times come back as strings like `07:23:18.331+00` (no minute in offset) and there's a bug in the
;; JDBC driver where it can't parse those correctly. We can do it ourselves in that case.
(m/defmethod rs/read-column-thunk [:postgresql :type/time]
  [^ResultSet rs rsmeta ^Integer i options]
  (let [parent-thunk ((get-method rs/read-column-thunk [:default :time]) rs rsmeta i options)]
    (fn []
      (try
        (parent-thunk)
        (catch Throwable _
          (let [s (.getString rs i)]
            (log/tracef "Error in Postgres JDBC driver reading TIME value, fetching as string '%s'" s)
            (second-date/parse s)))))))

;; The postgres JDBC driver cannot properly read MONEY columns — see https://github.com/pgjdbc/pgjdbc/issues/425. Work
;; around this by checking whether the column type name is `money`, and reading it out as a String and parsing to a
;; BigDecimal if so; otherwise, proceeding as normal
(m/defmethod rs/read-column-thunk [:postgresql :type/double]
  [^ResultSet rs ^ResultSetMetaData rsmeta ^Integer i _]
  (if (= (.getColumnTypeName rsmeta i) "money")
    (fn []
      (some-> (.getString rs i) u/parse-currency))
    (fn []
      (.getObject rs i))))
