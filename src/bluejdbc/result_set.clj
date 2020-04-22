(ns bluejdbc.result-set
  (:refer-clojure :exclude [type])
  (:require [bluejdbc.protocols :as protocols]
            [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [clojure.tools.logging :as log]
            [methodical.core :as m]
            [potemkin.types :as p.types]
            [pretty.core :as pretty])
  (:import [java.sql ResultSet ResultSetMetaData]))

(u/define-enums type            ResultSet #"^TYPE_"    :namespace :result-set-type)
(u/define-enums concurrency     ResultSet #"^CONCUR_"  :namespace :result-set-concurrency)
(u/define-enums holdability     ResultSet #"_CURSORS_" :namespace :result-set-holdability, :name-transform identity)
(u/define-enums fetch-direction ResultSet #"FETCH_")

(declare reduce-result-set result-set-seq)

(p.types/deftype+ ProxyResultSet [^ResultSet rs mta opts]
  pretty/PrettyPrintable
  (pretty [_]
    (list 'proxy-result-set rs opts))

  protocols/BlueJDBCProxy
  (options [_]
    opts)

  (with-options [_ new-options]
    (ProxyResultSet. rs mta new-options))

  clojure.lang.IObj
  (meta [_]
    mta)

  (withMeta [_ new-meta]
    (ProxyResultSet. rs new-meta opts))

  clojure.lang.IReduceInit
  (reduce [this rf init]
    (reduce-result-set this rf init))

  clojure.lang.IReduce
  (reduce [this rf]
    (reduce-result-set this rf []))

  clojure.lang.Seqable
  (seq [this]
    (result-set-seq this))

  java.sql.Wrapper
  (^boolean isWrapperFor [this ^Class interface]
   (or (instance? interface this)
       (.isWrapperFor rs interface)))

  (unwrap [this ^Class interface]
    (if (instance? interface this)
      this
      (.unwrap rs interface)))

  ResultSet
  (^boolean absolute [_ ^int a] (.absolute rs a))
  (^void afterLast [_] (.afterLast rs))
  (^void beforeFirst [_] (.beforeFirst rs))
  (^void cancelRowUpdates [_] (.cancelRowUpdates rs))
  (^void clearWarnings [_] (.clearWarnings rs))
  (^void close [_] (.close rs))
  (^void deleteRow [_] (.deleteRow rs))
  (^int findColumn [_ ^String a] (.findColumn rs a))
  (^boolean first [_] (.first rs))
  (^java.sql.Array getArray [_ ^int a] (.getArray rs a))
  (^java.sql.Array getArray [_ ^String a] (.getArray rs a))
  (^java.io.InputStream getAsciiStream [_ ^String a] (.getAsciiStream rs a))
  (^java.io.InputStream getAsciiStream [_ ^int a] (.getAsciiStream rs a))
  (^java.math.BigDecimal getBigDecimal [_ ^int a ^int b] (.getBigDecimal rs a b))
  (^java.math.BigDecimal getBigDecimal [_ ^String a ^int b] (.getBigDecimal rs a b))
  (^java.math.BigDecimal getBigDecimal [_ ^int a] (.getBigDecimal rs a))
  (^java.math.BigDecimal getBigDecimal [_ ^String a] (.getBigDecimal rs a))
  (^java.io.InputStream getBinaryStream [_ ^String a] (.getBinaryStream rs a))
  (^java.io.InputStream getBinaryStream [_ ^int a] (.getBinaryStream rs a))
  (^java.sql.Blob getBlob [_ ^String a] (.getBlob rs a))
  (^java.sql.Blob getBlob [_ ^int a] (.getBlob rs a))
  (^boolean getBoolean [_ ^String a] (.getBoolean rs a))
  (^boolean getBoolean [_ ^int a] (.getBoolean rs a))
  (^byte getByte [_ ^String a] (.getByte rs a))
  (^byte getByte [_ ^int a] (.getByte rs a))
  (^bytes getBytes [_ ^String a] (.getBytes rs a))
  (^bytes getBytes [_ ^int a] (.getBytes rs a))
  (^java.io.Reader getCharacterStream [_ ^String a] (.getCharacterStream rs a))
  (^java.io.Reader getCharacterStream [_ ^int a] (.getCharacterStream rs a))
  (^java.sql.Clob getClob [_ ^String a] (.getClob rs a))
  (^java.sql.Clob getClob [_ ^int a] (.getClob rs a))
  (^int getConcurrency [_] (.getConcurrency rs))
  (^String getCursorName [_] (.getCursorName rs))
  (^java.sql.Date getDate [_ ^String a] (.getDate rs a))
  (^java.sql.Date getDate [_ ^int a] (.getDate rs a))
  (^java.sql.Date getDate [_ ^String a ^java.util.Calendar b] (.getDate rs a b))
  (^java.sql.Date getDate [_ ^int a ^java.util.Calendar b] (.getDate rs a b))
  (^double getDouble [_ ^int a] (.getDouble rs a))
  (^double getDouble [_ ^String a] (.getDouble rs a))
  (^int getFetchDirection [_] (.getFetchDirection rs))
  (^int getFetchSize [_] (.getFetchSize rs))
  (^float getFloat [_ ^String a] (.getFloat rs a))
  (^float getFloat [_ ^int a] (.getFloat rs a))
  (^int getHoldability [_] (.getHoldability rs))
  (^int getInt [_ ^int a] (.getInt rs a))
  (^int getInt [_ ^String a] (.getInt rs a))
  (^long getLong [_ ^String a] (.getLong rs a))
  (^long getLong [_ ^int a] (.getLong rs a))
  (^java.sql.ResultSetMetaData getMetaData [_] (.getMetaData rs))
  (^java.io.Reader getNCharacterStream [_ ^String a] (.getNCharacterStream rs a))
  (^java.io.Reader getNCharacterStream [_ ^int a] (.getNCharacterStream rs a))
  (^java.sql.NClob getNClob [_ ^int a] (.getNClob rs a))
  (^java.sql.NClob getNClob [_ ^String a] (.getNClob rs a))
  (^String getNString [_ ^int a] (.getNString rs a))
  (^String getNString [_ ^String a] (.getNString rs a))
  (^Object getObject [_ ^String a] (.getObject rs a))
  (^Object getObject [_ ^int a ^java.util.Map b] (.getObject rs a b))
  (^Object getObject [_ ^int a] (.getObject rs a))
  (^Object getObject [_ ^String a ^java.util.Map b] (.getObject rs a b))
  (^Object getObject [_ ^int a ^Class b] (.getObject rs a b))
  (^Object getObject [_ ^String a ^Class b] (.getObject rs a b))
  (^java.sql.Ref getRef [_ ^int a] (.getRef rs a))
  (^java.sql.Ref getRef [_ ^String a] (.getRef rs a))
  (^int getRow [_] (.getRow rs))
  (^java.sql.RowId getRowId [_ ^String a] (.getRowId rs a))
  (^java.sql.RowId getRowId [_ ^int a] (.getRowId rs a))
  (^java.sql.SQLXML getSQLXML [_ ^String a] (.getSQLXML rs a))
  (^java.sql.SQLXML getSQLXML [_ ^int a] (.getSQLXML rs a))
  (^short getShort [_ ^String a] (.getShort rs a))
  (^short getShort [_ ^int a] (.getShort rs a))
  (^java.sql.Statement getStatement [_] (.getStatement rs))
  (^String getString [_ ^int a] (.getString rs a))
  (^String getString [_ ^String a] (.getString rs a))
  (^java.sql.Time getTime [_ ^String a ^java.util.Calendar b] (.getTime rs a b))
  (^java.sql.Time getTime [_ ^int a] (.getTime rs a))
  (^java.sql.Time getTime [_ ^int a ^java.util.Calendar b] (.getTime rs a b))
  (^java.sql.Time getTime [_ ^String a] (.getTime rs a))
  (^java.sql.Timestamp getTimestamp [_ ^String a ^java.util.Calendar b] (.getTimestamp rs a b))
  (^java.sql.Timestamp getTimestamp [_ ^int a ^java.util.Calendar b] (.getTimestamp rs a b))
  (^java.sql.Timestamp getTimestamp [_ ^String a] (.getTimestamp rs a))
  (^java.sql.Timestamp getTimestamp [_ ^int a] (.getTimestamp rs a))
  (^int getType [_] (.getType rs))
  (^java.net.URL getURL [_ ^String a] (.getURL rs a))
  (^java.net.URL getURL [_ ^int a] (.getURL rs a))
  (^java.io.InputStream getUnicodeStream [_ ^int a] (.getUnicodeStream rs a))
  (^java.io.InputStream getUnicodeStream [_ ^String a] (.getUnicodeStream rs a))
  (^java.sql.SQLWarning getWarnings [_] (.getWarnings rs))
  (^void insertRow [_] (.insertRow rs))
  (^boolean isAfterLast [_] (.isAfterLast rs))
  (^boolean isBeforeFirst [_] (.isBeforeFirst rs))
  (^boolean isClosed [_] (.isClosed rs))
  (^boolean isFirst [_] (.isFirst rs))
  (^boolean isLast [_] (.isLast rs))
  (^boolean last [_] (.last rs))
  (^void moveToCurrentRow [_] (.moveToCurrentRow rs))
  (^void moveToInsertRow [_] (.moveToInsertRow rs))
  (^boolean next [_] (.next rs))
  (^boolean previous [_] (.previous rs))
  (^void refreshRow [_] (.refreshRow rs))
  (^boolean relative [_ ^int a] (.relative rs a))
  (^boolean rowDeleted [_] (.rowDeleted rs))
  (^boolean rowInserted [_] (.rowInserted rs))
  (^boolean rowUpdated [_] (.rowUpdated rs))
  (^void setFetchDirection [_ ^int a] (.setFetchDirection rs a))
  (^void setFetchSize [_ ^int a] (.setFetchSize rs a))
  (^void updateArray [_ ^String a ^java.sql.Array b] (.updateArray rs a b))
  (^void updateArray [_ ^int a ^java.sql.Array b] (.updateArray rs a b))
  (^void updateAsciiStream [_ ^int a ^java.io.InputStream b ^long c] (.updateAsciiStream rs a b c))
  (^void updateAsciiStream [_ ^int a ^java.io.InputStream b ^int c] (.updateAsciiStream rs a b c))
  (^void updateAsciiStream [_ ^String a ^java.io.InputStream b ^long c] (.updateAsciiStream rs a b c))
  (^void updateAsciiStream [_ ^int a ^java.io.InputStream b] (.updateAsciiStream rs a b))
  (^void updateAsciiStream [_ ^String a ^java.io.InputStream b] (.updateAsciiStream rs a b))
  (^void updateAsciiStream [_ ^String a ^java.io.InputStream b ^int c] (.updateAsciiStream rs a b c))
  (^void updateBigDecimal [_ ^int a ^java.math.BigDecimal b] (.updateBigDecimal rs a b))
  (^void updateBigDecimal [_ ^String a ^java.math.BigDecimal b] (.updateBigDecimal rs a b))
  (^void updateBinaryStream [_ ^int a ^java.io.InputStream b ^long c] (.updateBinaryStream rs a b c))
  (^void updateBinaryStream [_ ^String a ^java.io.InputStream b ^int c] (.updateBinaryStream rs a b c))
  (^void updateBinaryStream [_ ^String a ^java.io.InputStream b] (.updateBinaryStream rs a b))
  (^void updateBinaryStream [_ ^String a ^java.io.InputStream b ^long c] (.updateBinaryStream rs a b c))
  (^void updateBinaryStream [_ ^int a ^java.io.InputStream b] (.updateBinaryStream rs a b))
  (^void updateBinaryStream [_ ^int a ^java.io.InputStream b ^int c] (.updateBinaryStream rs a b c))
  (^void updateBlob [_ ^int a ^java.io.InputStream b] (.updateBlob rs a b))
  (^void updateBlob [_ ^String a ^java.sql.Blob b] (.updateBlob rs a b))
  (^void updateBlob [_ ^String a ^java.io.InputStream b] (.updateBlob rs a b))
  (^void updateBlob [_ ^int a ^java.io.InputStream b ^long c] (.updateBlob rs a b c))
  (^void updateBlob [_ ^String a ^java.io.InputStream b ^long c] (.updateBlob rs a b c))
  (^void updateBlob [_ ^int a ^java.sql.Blob b] (.updateBlob rs a b))
  (^void updateBoolean [_ ^String a ^boolean b] (.updateBoolean rs a b))
  (^void updateBoolean [_ ^int a ^boolean b] (.updateBoolean rs a b))
  (^void updateByte [_ ^int a ^byte b] (.updateByte rs a b))
  (^void updateByte [_ ^String a ^byte b] (.updateByte rs a b))
  (^void updateBytes [_ ^int a ^bytes b] (.updateBytes rs a b))
  (^void updateBytes [_ ^String a ^bytes b] (.updateBytes rs a b))
  (^void updateCharacterStream [_ ^int a ^java.io.Reader b ^int c] (.updateCharacterStream rs a b c))
  (^void updateCharacterStream [_ ^String a ^java.io.Reader b] (.updateCharacterStream rs a b))
  (^void updateCharacterStream [_ ^int a ^java.io.Reader b ^long c] (.updateCharacterStream rs a b c))
  (^void updateCharacterStream [_ ^String a ^java.io.Reader b ^long c] (.updateCharacterStream rs a b c))
  (^void updateCharacterStream [_ ^int a ^java.io.Reader b] (.updateCharacterStream rs a b))
  (^void updateCharacterStream [_ ^String a ^java.io.Reader b ^int c] (.updateCharacterStream rs a b c))
  (^void updateClob [_ ^int a ^java.io.Reader b ^long c] (.updateClob rs a b c))
  (^void updateClob [_ ^String a ^java.sql.Clob b] (.updateClob rs a b))
  (^void updateClob [_ ^int a ^java.sql.Clob b] (.updateClob rs a b))
  (^void updateClob [_ ^String a ^java.io.Reader b] (.updateClob rs a b))
  (^void updateClob [_ ^int a ^java.io.Reader b] (.updateClob rs a b))
  (^void updateClob [_ ^String a ^java.io.Reader b ^long c] (.updateClob rs a b c))
  (^void updateDate [_ ^int a ^java.sql.Date b] (.updateDate rs a b))
  (^void updateDate [_ ^String a ^java.sql.Date b] (.updateDate rs a b))
  (^void updateDouble [_ ^int a ^double b] (.updateDouble rs a b))
  (^void updateDouble [_ ^String a ^double b] (.updateDouble rs a b))
  (^void updateFloat [_ ^String a ^float b] (.updateFloat rs a b))
  (^void updateFloat [_ ^int a ^float b] (.updateFloat rs a b))
  (^void updateInt [_ ^String a ^int b] (.updateInt rs a b))
  (^void updateInt [_ ^int a ^int b] (.updateInt rs a b))
  (^void updateLong [_ ^int a ^long b] (.updateLong rs a b))
  (^void updateLong [_ ^String a ^long b] (.updateLong rs a b))
  (^void updateNCharacterStream [_ ^int a ^java.io.Reader b] (.updateNCharacterStream rs a b))
  (^void updateNCharacterStream [_ ^int a ^java.io.Reader b ^long c] (.updateNCharacterStream rs a b c))
  (^void updateNCharacterStream [_ ^String a ^java.io.Reader b ^long c] (.updateNCharacterStream rs a b c))
  (^void updateNCharacterStream [_ ^String a ^java.io.Reader b] (.updateNCharacterStream rs a b))
  (^void updateNClob [_ ^int a ^java.sql.NClob b] (.updateNClob rs a b))
  (^void updateNClob [_ ^String a ^java.sql.NClob b] (.updateNClob rs a b))
  (^void updateNClob [_ ^int a ^java.io.Reader b] (.updateNClob rs a b))
  (^void updateNClob [_ ^String a ^java.io.Reader b] (.updateNClob rs a b))
  (^void updateNClob [_ ^String a ^java.io.Reader b ^long c] (.updateNClob rs a b c))
  (^void updateNClob [_ ^int a ^java.io.Reader b ^long c] (.updateNClob rs a b c))
  (^void updateNString [_ ^int a ^String b] (.updateNString rs a b))
  (^void updateNString [_ ^String a ^String b] (.updateNString rs a b))
  (^void updateNull [_ ^int a] (.updateNull rs a))
  (^void updateNull [_ ^String a] (.updateNull rs a))
  (^void updateObject [_ ^int a ^Object b ^java.sql.SQLType c ^int d] (.updateObject rs a b c d))
  (^void updateObject [_ ^int a ^Object b ^java.sql.SQLType c] (.updateObject rs a b c))
  (^void updateObject [_ ^String a ^Object b ^java.sql.SQLType c ^int d] (.updateObject rs a b c d))
  (^void updateObject [_ ^String a ^Object b ^java.sql.SQLType c] (.updateObject rs a b c))
  (^void updateObject [_ ^String a ^Object b ^int c] (.updateObject rs a b c))
  (^void updateObject [_ ^int a ^Object b] (.updateObject rs a b))
  (^void updateObject [_ ^int a ^Object b ^int c] (.updateObject rs a b c))
  (^void updateObject [_ ^String a ^Object b] (.updateObject rs a b))
  (^void updateRef [_ ^String a ^java.sql.Ref b] (.updateRef rs a b))
  (^void updateRef [_ ^int a ^java.sql.Ref b] (.updateRef rs a b))
  (^void updateRow [_] (.updateRow rs))
  (^void updateRowId [_ ^String a ^java.sql.RowId b] (.updateRowId rs a b))
  (^void updateRowId [_ ^int a ^java.sql.RowId b] (.updateRowId rs a b))
  (^void updateSQLXML [_ ^String a ^java.sql.SQLXML b] (.updateSQLXML rs a b))
  (^void updateSQLXML [_ ^int a ^java.sql.SQLXML b] (.updateSQLXML rs a b))
  (^void updateShort [_ ^int a ^short b] (.updateShort rs a b))
  (^void updateShort [_ ^String a ^short b] (.updateShort rs a b))
  (^void updateString [_ ^String a ^String b] (.updateString rs a b))
  (^void updateString [_ ^int a ^String b] (.updateString rs a b))
  (^void updateTime [_ ^String a ^java.sql.Time b] (.updateTime rs a b))
  (^void updateTime [_ ^int a ^java.sql.Time b] (.updateTime rs a b))
  (^void updateTimestamp [_ ^String a ^java.sql.Timestamp b] (.updateTimestamp rs a b))
  (^void updateTimestamp [_ ^int a ^java.sql.Timestamp b] (.updateTimestamp rs a b))
  (^boolean wasNull [_] (.wasNull rs)))

(defn proxy-result-set
  "Wrap `ResultSet` in a `ProxyResultSet`, if it is not already wrapped."
  (^ProxyResultSet [rs]
   (proxy-result-set rs nil))

  (^ProxyResultSet [rs options]
   (if (instance? ProxyResultSet rs)
     (protocols/with-options rs (merge (protocols/options rs) options))
     (ProxyResultSet. rs nil options))))

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^java.sql.ResultSet rs ^java.sql.ResultSetMetaData rsmeta ^Integer i options])}
  (fn [rs ^ResultSetMetaData rsmeta ^Integer i options]
    (let [col-type (u/reverse-lookup types/type (.getColumnType rsmeta i))]
      (log/tracef "Column %d %s is of type %s" i (pr-str (.getColumnLabel rsmeta i)) col-type)
      [(:connection/type options) col-type])))

(m/defmethod read-column-thunk :default
  [^ResultSet rs rsmeta ^Integer i options]
  (do
    (log/tracef "Fetching values in column %d with (.getObject rs %d)" i i)
    (fn []
      (.getObject rs i))))

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

(defn index-range
  "Return a sequence of indecies for all the columns in a result set. (`ResultSet` column indecies start at one in an
  effort to trip up developers.)"
  [^ResultSetMetaData rsmeta]
  (range 1 (inc (.getColumnCount rsmeta))))

(defn row-thunk
  "Returns a thunk that can be called repeatedly to get the next row in the result set, using appropriate methods to
  fetch each value in the row. Returns `nil` when the result set has no more rows."
  ([rs]
   (row-thunk rs (protocols/options rs)))

  ([^ResultSet rs options]
   (let [rsmeta (.getMetaData rs)
         fns    (for [i (index-range rsmeta)]
                  (read-column-thunk rs rsmeta (int i) options))]
     (let [thunk (apply juxt fns)]
       (fn row-thunk* []
         (when (.next rs)
           (thunk)))))))

(defn column-names
  "Return a sequence of column names (as keywords) for the rows in a `ResultSet`."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getColumnLabel rsmeta i))))))

(defn namespaced-column-names
  "Return a sequence of namespaced keyword column names for the rows in a `ResultSet`, e.g. `:table/column`."
  [^ResultSet rs]
  (let [rsmeta (.getMetaData rs)]
    (vec (for [i (index-range rsmeta)]
           (keyword (.getTableName rsmeta i) (.getColumnLabel rsmeta i))))))

(defn- maps-xform* [rs column-names]
  (fn [rf]
    (fn
      ([]
       (rf))

      ([acc]
       (rf acc))

      ([acc row]
       (rf acc (zipmap column-names row))))))

(defn maps-xform
  "A `ResultSet` transform that returns rows as maps with unqualified keys e.g. `:column`. This is the default and is
  applied if no other values of `:results/xform` are passed in options."
  [rs]
  (maps-xform* rs (column-names rs)))

(defn namespaced-maps-xform
  "A `ResultSet` transform that returns rows as maps with namespaced keys e.g.`:table/column`."
  [rs]
  (maps-xform* rs (namespaced-column-names rs)))

(defn reduce-result-set
  "Reduce the rows in a `ResultSet` using reducing function `rf`."
  {:arglists '([rs rf init] [rs options rf init])}
  ([rs rf init]
   (reduce-result-set rs (protocols/options rs) rf init))

  ([rs {xform :results/xform} rf init]
   (let [xform (if xform
                 (xform rs)
                 identity)
         rf    (xform rf)
         thunk (row-thunk rs)]
     (loop [acc init]
       (if (reduced? acc)
         @acc
         (if-let [row (thunk)]
           (recur (rf acc row))
           (do
             (log/trace "All rows consumed.")
             acc)))))))

(defn result-set-seq
  "Return a lazy sequence of rows from a `ResultSet`. Make sure you consume all the rows in the `ResultSet` while it is
  still open!

    ;; good
    (with-open [rs (jdbc/results stmt)]
      (doseq [row (result-set-seq rs)]
        ...))

    ;; bad -- ResultSet is closed before we finish the `doseq` loop
    (doseq [row (with-open [rs (jdbc/results stmt)]
                  (jdbc/results stmt))]
      ...)"
  {:arglists '(^clojure.lang.ISeq [rs]
               ^clojure.lang.ISeq [rs options])}
  ([rs]
   (result-set-seq rs (protocols/options rs)))

  ([rs {xform :results/xform, :or {xform maps-xform}}]
   (let [xform        (if xform
                        (xform rs)
                        identity)
         rf           (xform (fn [_ row] row))
         thunk        (row-thunk rs)
         ;; TODO -- should this be `^:once fn*` ??
         lazy-results (fn lazy-results []
                        (lazy-seq
                         (when-let [row (thunk)]
                           (cons (rf nil row) (lazy-results)))))]
     (lazy-results))))
