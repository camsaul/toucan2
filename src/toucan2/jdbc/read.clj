(ns toucan2.jdbc.read
  "[[read-column-thunk]] method, which is used to determine how to read values of columns in results, and default
  implementations"
  (:require
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.util :as u])
  (:import
   (java.sql Connection ResultSet ResultSetMetaData Types)))

(set! *warn-on-reflection* true)

;;; arglists metadata is mostly so (theoretically) Kondo can catch if you try to call this with the wrong type or wrong
;;; number of args.
(def ^{:arglists '(^String [^Integer i] ^String [^Integer i not-found])} type-name
  "Map of `java.sql.Types` enum integers (e.g. `java.sql.Types/FLOAT`, whose value is `6`) to the string type name e.g.
  `FLOAT`.

  ```clj
  (type-name java.sql.Types/FLOAT) -> (type-name 6) -> \"FLOAT\"
  ```"
  (into {} (for [^java.lang.reflect.Field field (.getDeclaredFields Types)]
             [(.getLong field Types) (.getName field)])))

;;; TODO -- dispatch for this method is busted.
;;;
;;; 1.`col-type` should be an explicit parameter. A little weird to dispatch off of something that's not even one of the
;;;    parameters
;;;
;;; 2. This should also dispatch off of the actual underlying column name
(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row."
  {:arglists '([^Connection conn₁ model₂ ^ResultSet rset ^ResultSetMetaData rsmeta₍₃₎ ^Long i])}
  (fn [^Connection conn model _rset ^ResultSetMetaData rsmeta ^Long i]
    (let [col-type (.getColumnType rsmeta i)]
      (log/debugf :results
                  "Column %s %s is of JDBC type %s, native type %s"
                  i
                  (str (.getTableName rsmeta i) \. (.getColumnLabel rsmeta i))
                  (symbol "java.sql.Types" (type-name col-type))
                  (.getColumnTypeName rsmeta i))
      [(protocols/dispatch-value conn) (protocols/dispatch-value model) col-type])))

(m/defmethod read-column-thunk :default
  [_conn _model ^ResultSet rset _rsmeta ^Long i]
  (log/debugf :results "Fetching values in column %s with %s" i (list '.getObject 'rs i))
  (fn default-read-column-thunk []
    (log/tracef :results "col %s => %s" i (list '.getObject 'rset i))
    (.getObject rset i)))

(m/defmethod read-column-thunk :after :default
  [_conn model _rset _rsmeta thunk]
  (fn []
    (u/try-with-error-context ["read column" {:thunk thunk, :model model}]
      (thunk))))

(defn get-object-of-class-thunk [^ResultSet rset ^Long i ^Class klass]
  (log/debugf :results
              "Fetching values in column %s with %s"
              i
              (list '.getObject 'rs i klass))
  (fn get-object-of-class-thunk []
    (log/tracef :results "col %s => %s" i (list '.getObject 'rset i klass))
    (.getObject rset i klass)))

;;;; Default column read methods

(m/defmethod read-column-thunk [:default :default Types/CLOB]
  [_conn _model ^ResultSet rset _ ^Long i]
  (fn get-string-thunk []
    (.getString rset i)))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDateTime))

(m/defmethod read-column-thunk [:default :default Types/TIMESTAMP_WITH_TIMEZONE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.OffsetDateTime))

(m/defmethod read-column-thunk [:default :default Types/DATE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalDate))

(m/defmethod read-column-thunk [:default :default Types/TIME]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.LocalTime))

(m/defmethod read-column-thunk [:default :default Types/TIME_WITH_TIMEZONE]
  [_conn _model rset _rsmeta i]
  (get-object-of-class-thunk rset i java.time.OffsetTime))

(defn- make-column-thunk [conn model ^ResultSet rset i]
  (log/tracef :results "Building thunk to read column %s" i)
  (fn column-thunk []
    (let [rsmeta (.getMetaData rset)
          thunk  (read-column-thunk conn model rset rsmeta i)
          v      (thunk)]
      (next.jdbc.rs/read-column-by-index v rsmeta i))))

(defn- make-i->thunk [conn model rset]
  (comp (memoize (fn [i]
                   (make-column-thunk conn model rset i)))
        int))

(defn make-cached-row-num->i->thunk [conn model ^ResultSet rset]
  (let [i->thunk       (make-i->thunk conn model rset)
        cached-row-num (atom -1)
        cached-values  (atom {})]
    (fn row-num->i->thunk* [current-row-num]
      (when-not (= current-row-num @cached-row-num)
        (reset! cached-row-num current-row-num)
        (reset! cached-values {}))
      (fn i->thunk* [i]
        (fn cached-column-thunk []
          (let [cached-value (get @cached-values i ::not-found)]
            (if-not (= cached-value ::not-found)
              (log/tracef :results "Using cached value for column %s: %s" i cached-value)
              cached-value)
            (let [thunk (i->thunk i)
                  v     (thunk)]
              (swap! cached-values assoc i v)
              v)))))))

(defn read-column-by-index-fn
  "Given a `java.sql.Connection` `conn`, a `model`, and a `java.sql.ResultSet` `rset`, return a function that can be used
  with [[next.jdbc.result-set/builder-adapter]]. The function has the signature

  ```clj
  (f builder rset i) => result
  ```

  When this function is called with a [[next.jdbc]] result set `builder`, a `java.sql.ResultSet` `rset`, and column
  index `i`, it will return the value of that column for the current row.

  The function used to fetch the column is built by combining [[read-column-thunk]]
  and [[next.jdbc.result-set/read-column-by-index]]; the function is built once and used repeatedly for each new row.

  Values are cached for the current row -- fetching the same column twice for a given row will only result in fetching
  it from the database once."
  ([conn model ^ResultSet rset]
   (read-column-by-index-fn (make-cached-row-num->i->thunk conn model rset)))

  ([row-num->i->thunk]
   (fn read-column-by-index-fn* [_builder ^ResultSet rset ^Integer i]
     (assert (not (.isClosed ^ResultSet rset))
             "ResultSet is already closed. Do you need call toucan2.realize/realize on the row before the Connection is closed?")
     (let [i->thunk (row-num->i->thunk (.getRow rset))
           thunk    (i->thunk i)
           result   (thunk)]
       (log/tracef :results "col %s => %s" i result)
       result))))

;;;; Postgres integration

;;; TODO -- why is cljdoc picking this up?
;;;
;;; TODO -- `org.postgresql.PGConnection` is the interface; `org.postgresql.jdbc.PgConnection` is actual concrete class.
;;; Are there other concrete classes we might want to handle here? :
(when-let [pg-connection-class (try
                                 (Class/forName "org.postgresql.jdbc.PgConnection")
                                 (catch Throwable _
                                   nil))]
  (m/defmethod read-column-thunk [pg-connection-class :default Types/TIMESTAMP]
    [_conn _model ^ResultSet rset ^ResultSetMetaData rsmeta ^Long i]
    (let [^Class klass (if (= (.getColumnTypeName rsmeta i) "timestamptz")
                         java.time.OffsetDateTime
                         java.time.LocalDateTime)]
      (get-object-of-class-thunk rset i klass))))
