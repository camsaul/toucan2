(ns bluejdbc.statement
  "Protocols, methods, functions, macros, etc. for working with `java.sql.PreparedStatement`s."
  (:require [bluejdbc.options :as options]
            [bluejdbc.protocols :as protocols]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [methodical.core :as m]
            [potemkin.types :as p.types]
            [pretty.core :as pretty])
  (:import [java.sql Connection PreparedStatement ResultSet Statement]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]))

(p.types/deftype+ ProxyPreparedStatement [^PreparedStatement stmt mta opts]
  pretty/PrettyPrintable
  (pretty [_]
    (list 'proxy-prepared-statement stmt opts))

  protocols/BlueJDBCProxy
  (options [_]
    opts)

  (with-options [_ new-options]
    (ProxyPreparedStatement. stmt mta new-options))

  clojure.lang.IObj
  (meta [_]
    mta)

  (withMeta [_ new-meta]
    (ProxyPreparedStatement. stmt new-meta opts))

  clojure.lang.IReduceInit
  (reduce [this rf init]
    (with-open [rs (.executeQuery this)]
      (reduce rf init rs)))

  clojure.lang.IReduce
  (reduce [this rf]
    (with-open [rs (.executeQuery this)]
      (reduce rf [] rs)))

  java.sql.Wrapper
  (^boolean isWrapperFor [this ^Class interface]
   (or (instance? interface this)
       (.isWrapperFor stmt interface)))

  (unwrap [this ^Class interface]
    (if (instance? interface this)
      this
      (.unwrap stmt interface)))

  PreparedStatement
  (^void addBatch [_] (.addBatch stmt))
  (^void addBatch [_ ^String a] (.addBatch stmt a))
  (^void cancel [_] (.cancel stmt))
  (^void clearBatch [_] (.clearBatch stmt))
  (^void clearParameters [_] (.clearParameters stmt))
  (^void clearWarnings [_] (.clearWarnings stmt))
  (^void close [_] (.close stmt))
  (^void closeOnCompletion [_] (.closeOnCompletion stmt))
  (^String enquoteIdentifier [_ ^String a ^boolean b] (.enquoteIdentifier stmt a b))
  (^String enquoteLiteral [_ ^String a] (.enquoteLiteral stmt a))
  (^String enquoteNCharLiteral [_ ^String a] (.enquoteNCharLiteral stmt a))
  (^boolean execute [_] (.execute stmt))
  (^boolean execute [_ ^String a ^int b] (.execute stmt a b))
  (^boolean execute [_ ^String a ^ints b] (.execute stmt a b))
  (^boolean execute [_ ^String a ^ "[Ljava.lang.String;" b] (.execute stmt a b))
  (^boolean execute [_ ^String a] (.execute stmt a))
  (^ints executeBatch [_] (.executeBatch stmt))
  (^longs executeLargeBatch [_] (.executeLargeBatch stmt))
  (^long executeLargeUpdate [_] (.executeLargeUpdate stmt))
  (^long executeLargeUpdate [_ ^String a ^ "[Ljava.lang.String;" b] (.executeLargeUpdate stmt a b))
  (^long executeLargeUpdate [_ ^String a ^ints b] (.executeLargeUpdate stmt a b))
  (^long executeLargeUpdate [_ ^String a ^int b] (.executeLargeUpdate stmt a b))
  (^long executeLargeUpdate [_ ^String a] (.executeLargeUpdate stmt a))

  (^java.sql.ResultSet executeQuery
   [_]
   (-> (.executeQuery stmt)
       (options/set-options! opts)
       (result-set/proxy-result-set opts)))

  ;; cannot actually be called on a `PreparedStatement`
  (^java.sql.ResultSet executeQuery [_ ^String a] (.executeQuery stmt a))
  (^int executeUpdate [_] (.executeUpdate stmt))
  (^int executeUpdate [_ ^String a ^ "[Ljava.lang.String;" b] (.executeUpdate stmt a b))
  (^int executeUpdate [_ ^String a ^ints b] (.executeUpdate stmt a b))
  (^int executeUpdate [_ ^String a] (.executeUpdate stmt a))
  (^int executeUpdate [_ ^String a ^int b] (.executeUpdate stmt a b))
  (^java.sql.Connection getConnection [_] (.getConnection stmt))
  (^int getFetchDirection [_] (.getFetchDirection stmt))
  (^int getFetchSize [_] (.getFetchSize stmt))
  (^java.sql.ResultSet getGeneratedKeys [_] (.getGeneratedKeys stmt))
  (^long getLargeMaxRows [_] (.getLargeMaxRows stmt))
  (^long getLargeUpdateCount [_] (.getLargeUpdateCount stmt))
  (^int getMaxFieldSize [_] (.getMaxFieldSize stmt))
  (^int getMaxRows [_] (.getMaxRows stmt))
  (^java.sql.ResultSetMetaData getMetaData [_] (.getMetaData stmt))
  (^boolean getMoreResults [_] (.getMoreResults stmt))
  (^boolean getMoreResults [_ ^int a] (.getMoreResults stmt a))
  (^java.sql.ParameterMetaData getParameterMetaData [_] (.getParameterMetaData stmt))
  (^int getQueryTimeout [_] (.getQueryTimeout stmt))
  (^java.sql.ResultSet getResultSet [_] (.getResultSet stmt))
  (^int getResultSetConcurrency [_] (.getResultSetConcurrency stmt))
  (^int getResultSetHoldability [_] (.getResultSetHoldability stmt))
  (^int getResultSetType [_] (.getResultSetType stmt))
  (^int getUpdateCount [_] (.getUpdateCount stmt))
  (^java.sql.SQLWarning getWarnings [_] (.getWarnings stmt))
  (^boolean isCloseOnCompletion [_] (.isCloseOnCompletion stmt))
  (^boolean isClosed [_] (.isClosed stmt))
  (^boolean isPoolable [_] (.isPoolable stmt))
  (^boolean isSimpleIdentifier [_ ^String a] (.isSimpleIdentifier stmt a))
  (^void setArray [_ ^int a ^java.sql.Array b] (.setArray stmt a b))
  (^void setAsciiStream [_ ^int a ^java.io.InputStream b] (.setAsciiStream stmt a b))
  (^void setAsciiStream [_ ^int a ^java.io.InputStream b ^int c] (.setAsciiStream stmt a b c))
  (^void setAsciiStream [_ ^int a ^java.io.InputStream b ^long c] (.setAsciiStream stmt a b c))
  (^void setBigDecimal [_ ^int a ^java.math.BigDecimal b] (.setBigDecimal stmt a b))
  (^void setBinaryStream [_ ^int a ^java.io.InputStream b ^long c] (.setBinaryStream stmt a b c))
  (^void setBinaryStream [_ ^int a ^java.io.InputStream b] (.setBinaryStream stmt a b))
  (^void setBinaryStream [_ ^int a ^java.io.InputStream b ^int c] (.setBinaryStream stmt a b c))
  (^void setBlob [_ ^int a ^java.io.InputStream b ^long c] (.setBlob stmt a b c))
  (^void setBlob [_ ^int a ^java.io.InputStream b] (.setBlob stmt a b))
  (^void setBlob [_ ^int a ^java.sql.Blob b] (.setBlob stmt a b))
  (^void setBoolean [_ ^int a ^boolean b] (.setBoolean stmt a b))
  (^void setByte [_ ^int a ^byte b] (.setByte stmt a b))
  (^void setBytes [_ ^int a ^bytes b] (.setBytes stmt a b))
  (^void setCharacterStream [_ ^int a ^java.io.Reader b ^int c] (.setCharacterStream stmt a b c))
  (^void setCharacterStream [_ ^int a ^java.io.Reader b ^long c] (.setCharacterStream stmt a b c))
  (^void setCharacterStream [_ ^int a ^java.io.Reader b] (.setCharacterStream stmt a b))
  (^void setClob [_ ^int a ^java.sql.Clob b] (.setClob stmt a b))
  (^void setClob [_ ^int a ^java.io.Reader b] (.setClob stmt a b))
  (^void setClob [_ ^int a ^java.io.Reader b ^long c] (.setClob stmt a b c))
  (^void setCursorName [_ ^String a] (.setCursorName stmt a))
  (^void setDate [_ ^int a ^java.sql.Date b ^java.util.Calendar c] (.setDate stmt a b c))
  (^void setDate [_ ^int a ^java.sql.Date b] (.setDate stmt a b))
  (^void setDouble [_ ^int a ^double b] (.setDouble stmt a b))
  (^void setEscapeProcessing [_ ^boolean a] (.setEscapeProcessing stmt a))
  (^void setFetchDirection [_ ^int a] (.setFetchDirection stmt a))
  (^void setFetchSize [_ ^int a] (.setFetchSize stmt a))
  (^void setFloat [_ ^int a ^float b] (.setFloat stmt a b))
  (^void setInt [_ ^int a ^int b] (.setInt stmt a b))
  (^void setLargeMaxRows [_ ^long a] (.setLargeMaxRows stmt a))
  (^void setLong [_ ^int a ^long b] (.setLong stmt a b))
  (^void setMaxFieldSize [_ ^int a] (.setMaxFieldSize stmt a))
  (^void setMaxRows [_ ^int a] (.setMaxRows stmt a))
  (^void setNCharacterStream [_ ^int a ^java.io.Reader b ^long c] (.setNCharacterStream stmt a b c))
  (^void setNCharacterStream [_ ^int a ^java.io.Reader b] (.setNCharacterStream stmt a b))
  (^void setNClob [_ ^int a ^java.sql.NClob b] (.setNClob stmt a b))
  (^void setNClob [_ ^int a ^java.io.Reader b ^long c] (.setNClob stmt a b c))
  (^void setNClob [_ ^int a ^java.io.Reader b] (.setNClob stmt a b))
  (^void setNString [_ ^int a ^String b] (.setNString stmt a b))
  (^void setNull [_ ^int a ^int b ^String c] (.setNull stmt a b c))
  (^void setNull [_ ^int a ^int b] (.setNull stmt a b))
  (^void setObject [_ ^int a ^Object b ^int c] (.setObject stmt a b c))
  (^void setObject [_ ^int a ^Object b] (.setObject stmt a b))
  (^void setObject [_ ^int a ^Object b ^java.sql.SQLType c] (.setObject stmt a b c))
  (^void setObject [_ ^int a ^Object b ^int c ^int d] (.setObject stmt a b c d))
  (^void setObject [_ ^int a ^Object b ^java.sql.SQLType c ^int d] (.setObject stmt a b c d))
  (^void setPoolable [_ ^boolean a] (.setPoolable stmt a))
  (^void setQueryTimeout [_ ^int a] (.setQueryTimeout stmt a))
  (^void setRef [_ ^int a ^java.sql.Ref b] (.setRef stmt a b))
  (^void setRowId [_ ^int a ^java.sql.RowId b] (.setRowId stmt a b))
  (^void setSQLXML [_ ^int a ^java.sql.SQLXML b] (.setSQLXML stmt a b))
  (^void setShort [_ ^int a ^short b] (.setShort stmt a b))
  (^void setString [_ ^int a ^String b] (.setString stmt a b))
  (^void setTime [_ ^int a ^java.sql.Time b ^java.util.Calendar c] (.setTime stmt a b c))
  (^void setTime [_ ^int a ^java.sql.Time b] (.setTime stmt a b))
  (^void setTimestamp [_ ^int a ^java.sql.Timestamp b] (.setTimestamp stmt a b))
  (^void setTimestamp [_ ^int a ^java.sql.Timestamp b ^java.util.Calendar c] (.setTimestamp stmt a b c))
  (^void setURL [_ ^int a ^java.net.URL b] (.setURL stmt a b))
  (^void setUnicodeStream [_ ^int a ^java.io.InputStream b ^int c] (.setUnicodeStream stmt a b c)))

(defn proxy-prepared-statement
  "Wrap `PreparedStatement` in a `ProxyPreparedStatment`, if it is not already wrapped."
  (^ProxyPreparedStatement [stmt]
   (proxy-prepared-statement stmt nil))

  (^ProxyPreparedStatement [stmt options]
   (when stmt
     (if (instance? ProxyPreparedStatement stmt)
       (protocols/with-options stmt (merge (protocols/options stmt) options))
       (ProxyPreparedStatement. stmt nil options)))))

(defn set-object!
  ([^PreparedStatement stmt ^Integer index object]
   (log/tracef "(set-object! stmt %d ^%s %s)" index (some-> object class .getCanonicalName) (pr-str object))
   (.setObject stmt index object))

  ([^PreparedStatement stmt ^Integer index object target-sql-type]
   (log/tracef "(set-object! stmt %d ^%s %s %s)" index
               (some-> object class .getCanonicalName) (pr-str object)
               (u/reverse-lookup types/type target-sql-type))
   (.setObject stmt index object (types/type target-sql-type))))

(m/defmulti set-parameter!
  {:arglists '([^PreparedStatement stmt ^Integer i object options])}
  (fn [_ _ object options]
    [(:connection/type options) (class object)]))

(m/defmethod set-parameter! :default
  [stmt i object options]
  (set-object! stmt i object))

(m/defmethod set-parameter! [:default LocalDate]
  [stmt i t _]
  (set-object! stmt i t :date))

(m/defmethod set-parameter! [:default LocalTime]
  [stmt i t _]
  (set-object! stmt i t :time))

(m/defmethod set-parameter! [:default LocalDateTime]
  [stmt i t _]
  (set-object! stmt i t :timestamp))

(m/defmethod set-parameter! [:default OffsetTime]
  [stmt i t _]
  (set-object! stmt i t :time-with-timezone))

(m/defmethod set-parameter! [:default OffsetDateTime]
  [stmt i t _]
  (set-object! stmt i t :timestamp-with-timezone))

(m/defmethod set-parameter! [:default ZonedDateTime]
  [stmt i t _]
  (set-object! stmt i t :timestamp-with-timezone))

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

(p.types/defprotocol+ CreatePreparedStatement
  (prepare!* ^bluejdbc.statement.ProxyPreparedStatement [this ^Connection conn options]))

(defn format-honeysql
  "Compile a HoneySQL form into `[sql & args]`."
  [honeysql-form options]
  (let [honeysql-options (into {} (for [[k v] options
                                        :when (= (namespace k) "honeysql")]
                                    [(keyword (name k)) v]))]
    (log/tracef "Compile HoneySQL form\n%s\noptions: %s" (u/pprint-to-str honeysql-form) (u/pprint-to-str honeysql-options))
    (let [sql-args (apply hsql/format honeysql-form (mapcat identity honeysql-options))]
      (log/tracef "->\n%s" (u/pprint-to-str sql-args))
      sql-args)))

(extend-protocol CreatePreparedStatement
  ;; plain SQL string
  String
  (prepare!* [s ^Connection conn {rs-type     :result-set/type
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
      (options/set-options! stmt options)
      (proxy-prepared-statement stmt options)))

  ;; [sql & args] or [honeysql & args] vector
  clojure.lang.Sequential
  (prepare!* [[query & params] conn options]
    (-> (prepare!* query conn options)
        (set-parameters! params options)))

  ;; HoneySQL map
  clojure.lang.IPersistentMap
  (prepare!* [honeysql-form conn options]
    (prepare!* (format-honeysql honeysql-form options) conn options)))

(defn prepare!
  "Create a new `PreparedStatement` from `query`. `query` is one of:

    *  A SQL string
    *  A HoneySQL Form
    *  [sql & parameters] or [honeysql-form & parameters]
    *  A `PreparedStatement`
    *  Anything else that you've added that implements `bluejdbc.statement.CreatePreparedStatement`"
  (^ProxyPreparedStatement [conn query]
   (prepare! conn query nil))

  (^ProxyPreparedStatement [conn query options]
   (assert (instance? java.sql.Connection conn)
           (str "Not a Connection: " (class conn)))
   (prepare!* query conn (merge (protocols/options conn) options))))

(defn do-with-prepared-statement
  "Impl for `with-prepared-statement`."
  [conn query-or-stmt options f]
  (if (instance? PreparedStatement query-or-stmt)
    (let [stmt (proxy-prepared-statement query-or-stmt options)]
      (f stmt))
    (with-open [stmt (prepare! conn query-or-stmt options)]
      (f stmt))))

(defmacro with-prepared-statement
  "Execute `body` with `stmt-binding` bound to a `PreparedStatement`. If `query-or-stmt` is already a
  `PreparedStatement`, `body` is executed using that `PreparedStatement`; if `query-or-stmt` is something else such as
  a SQL or HoneySQL query, a new `PreparedStatement` will be created for the duration of `body` and closed afterward.

  You can use this macro to accept either a `PreparedStatement` or a SQL/HoneySQL query and handle either case
  appropriately."
  {:arglists '([[stmt-binding conn query-or-stmt] & body] [[stmt-binding conn query-or-stmt options] & body])}
  [[stmt-binding conn query-or-stmt options] & body]
  `(do-with-prepared-statement
    ~conn ~query-or-stmt ~options
    (fn [~(vary-meta stmt-binding assoc :tag 'bluejdbc.statement.ProxyPreparedStatement)]
      ~@body)))

(defn results
  "Execute a `PreparedStatement` containing a *query*, such a `SELECT` statement. Returns a `ProxyResultSet`, which is
  reducible."
  (^bluejdbc.result_set.ProxyResultSet [stmt]
   (results stmt nil))

  (^bluejdbc.result_set.ProxyResultSet [stmt options]
   (.executeQuery (proxy-prepared-statement stmt options))))


;;;; Option handling

(m/defmethod options/set-option! [Statement :statement/max-rows]
  [^Statement stmt _ ^Integer max-rows]
  (when max-rows
    (log/tracef "Set Statement max rows -> %d" max-rows)
    (.setMaxRows stmt max-rows)))
