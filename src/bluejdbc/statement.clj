(ns bluejdbc.statement
  "Protocols, methods, functions, macros, etc. for working with `java.sql.PreparedStatement`s."
  (:require [bluejdbc.options :as options]
            [bluejdbc.result-set :as rs]
            [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [bluejdbc.util.log :as log]
            [honeysql.core :as hsql]
            [java-time :as t]
            [methodical.core :as m]
            [potemkin.types :as p.types]
            [pretty.core :as pretty])
  (:import [java.sql Connection PreparedStatement ResultSet Statement]
           [java.time Instant LocalDate LocalDateTime LocalTime OffsetDateTime OffsetTime ZonedDateTime]))

;;;; Parameters

(defn set-object!
  "Set `PreparedStatement` parameter at index `i` to `object` by calling `.setObject`, optionally with a
  `target-sql-type` (which may be either the raw Java enum integer, e.g. `java.sql.Types/INTEGER`, or the keyword name
  of it, e.g. `:integer`.)"
  ([^PreparedStatement stmt ^Integer i object]
   (log/tracef "(set-object! stmt %d ^%s %s)" i (some-> object class .getCanonicalName) (pr-str object))
   (.setObject stmt i object))

  ([^PreparedStatement stmt ^Integer i object target-sql-type]
   (log/tracef "(set-object! stmt %d ^%s %s %s)" i
               (some-> object class .getCanonicalName) (pr-str object)
               (u/reverse-lookup types/type target-sql-type))
   (.setObject stmt i object (types/type target-sql-type))))

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


;;;; Proxy Prepared Statement

(u/define-proxy-class ProxyPreparedStatement PreparedStatement [stmt mta opts]
  pretty/PrettyPrintable
  (pretty [_]
    (list 'proxy-prepared-statement stmt opts))

  options/Options
  (options [_]
    opts)

  (with-options* [_ new-options]
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

  PreparedStatement
  (^java.sql.ResultSet getGeneratedKeys
   [this]
   (rs/reducible-result-set (.getGeneratedKeys stmt) (assoc opts :_statement this)))

  (^java.sql.ResultSet executeQuery
   [this]
   (rs/reducible-result-set (.executeQuery stmt) (assoc opts :_statement this)))

  (^java.sql.Connection getConnection [_]
   (or (:_connection opts)
       (.getConnection stmt))))

(defn proxy-prepared-statement
  "Wrap `PreparedStatement` in a `ProxyPreparedStatment`, if it is not already wrapped."
  (^ProxyPreparedStatement [stmt & [options]]
   (u/proxy-wrap ProxyPreparedStatement ->ProxyPreparedStatement stmt options)))

(p.types/defprotocol+ CreatePreparedStatement
  "Protocol for anything that can be used to create a `PreparedStatement` in combination with a `Connection`."
  (prepare!* ^bluejdbc.statement.ProxyPreparedStatement [this ^Connection conn options]
    "Create a `ProxyPreparedStatement` from `this` for `Connection` `conn`. This is a low-level method -- unless adding a
    new implementation, use the higher-level `prepare!`."))

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

(defn- quoting-strategy [^Connection conn]
  (case (.. conn getMetaData getIdentifierQuoteString)
    "\"" :ansi
    "`"  :mysql
    nil))

(extend-protocol CreatePreparedStatement
  ;; plain SQL string
  String
  (prepare!* [s ^Connection conn {rs-type               :result-set/type
                                  concurrency           :result-set/concurrency
                                  holdability           :result-set/holdability
                                  return-generated-keys :statement/return-generated-keys
                                  :or                   {rs-type     :forward-only
                                                         concurrency :read-only
                                                         holdability (.getHoldability conn)}
                                  :as                   options}]
    (let [stmt (cond
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
                                            (u/reverse-lookup rs/type rs-type)
                                            (u/reverse-lookup rs/concurrency concurrency)
                                            (u/reverse-lookup rs/holdability holdability))))
                   (.prepareStatement conn
                                      s
                                      (rs/type rs-type)
                                      (rs/concurrency concurrency)
                                      (rs/holdability holdability))))]
      (proxy-prepared-statement stmt options)))

  ;; [sql & args] or [honeysql & args] vector
  clojure.lang.Sequential
  (prepare!* [[query & params] conn options]
    (-> (prepare!* query conn options)
        (set-parameters! params options)))

  ;; HoneySQL map
  clojure.lang.IPersistentMap
  (prepare!* [honeysql-form conn options]
    (prepare!* (format-honeysql honeysql-form options) conn
               (cond-> options
                 (not (:honeysql/quoting options)) (assoc :honeysql/quoting (quoting-strategy conn))))))

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
   (prepare!* query conn (merge (options/options conn) options))))

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
    (.setMaxRows stmt max-rows)))
