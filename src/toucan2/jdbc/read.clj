(ns toucan2.jdbc.read
  "[[read-column-thunk]] method, which is used to determine how to read values of columns in results, and default
  implementations"
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [next.jdbc.result-set :as next.jdbc.rs]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.types :as types]
   [toucan2.util :as u])
  (:import
   (java.sql Connection ResultSet ResultSetMetaData Types)))

(comment s/keep-me
         types/keep-me)

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

(m/defmulti read-column-thunk
  "Return a zero-arg function that, when called, will fetch the value of the column from the current row.

  Dispatches on `java.sql.Connection` class, `model`, and the `java.sql.Types` mapping for the column, e.g.
  `java.sql.Types/TIMESTAMP`.

  ### TODO -- dispatch for this method is busted.

  1. The `java.sql.Types` column type should be an explicit parameter. A little weird to dispatch off of something
     that's not even one of the parameters

  2. Should this also dispatch off of the actual underlying column name? So you can read a column in different ways for
     different models.

  3. Should this dispatch off of the underlying database column type name string, e.g. `timestamp` or `timestamptz`? It
     seems like a lot of the time we need to do different things based on that type name."
  {:arglists            '([^Connection conn₁ model₂ ^ResultSet rset ^ResultSetMetaData rsmeta₍₃₎ ^Long i])
   :defmethod-arities   #{5}
   :dispatch-value-spec (types/or-default-spec
                         (s/cat :connection-class ::types/dispatch-value.keyword-or-class
                                :model            ::types/dispatch-value.model
                                :column-type      any?))}
  (fn [^Connection conn model _rset ^ResultSetMetaData rsmeta ^Long i]
    (let [col-type (.getColumnType rsmeta i)]
      (log/debugf
       "Column %s %s is of JDBC type %s, native type %s"
       i
       (let [table-name  (some->> (.getTableName rsmeta i) not-empty)
             column-name (.getColumnLabel rsmeta i)]
         (if table-name
           (str table-name \. column-name)
           column-name))
       (symbol "java.sql.Types" (type-name col-type))
       (.getColumnTypeName rsmeta i))
      [(protocols/dispatch-value conn) (protocols/dispatch-value model) col-type])))

(m/defmethod read-column-thunk :default
  [_conn _model ^ResultSet rset _rsmeta ^Long i]
  (log/debugf "Fetching values in column %s with %s" i (list '.getObject 'rs i))
  (fn default-read-column-thunk []
    (log/tracef "col %s => %s" i (list '.getObject 'rset i))
    (.getObject rset i)))

(m/defmethod read-column-thunk :after :default
  [_conn model _rset _rsmeta thunk]
  (fn []
    (u/try-with-error-context ["read column" {:thunk thunk, :model model}]
      (thunk))))

(defn get-object-of-class-thunk
  "Return a thunk that will be used to fetch values at column index `i` from ResultSet `rset` as a given class. Calling
  this thunk is equivalent to

  ```clj
  (.getObject rset i klass)
  ```

  but includes extra logging."
  [^ResultSet rset ^Long i ^Class klass]
  (log/debugf
              "Fetching values in column %s with %s"
              i
              (list '.getObject 'rs i klass))
  (fn get-object-of-class-thunk []
    ;; what's the overhead of this? A million rows with 10 columns each = 10 million calls =(
    ;;
    ;; from Criterium: a no-op call takes about 20ns locally. So 10m rows => 200ms from this no-op call. That's a little
    ;; expensive, but probably not as bad as the overhead we get from other nonsense here in Toucan 2. We'll have to do
    ;; some general performance tuning in the future.
    (log/tracef "col %s => %s" i (list '.getObject 'rset i klass))
    (.getObject rset i klass)))

;;;; Default column read methods

(m/defmethod read-column-thunk [:default :default Types/CLOB]
  [_conn _model ^ResultSet rset _ ^Long i]
  (fn get-string-thunk []
    (.getString rset i)))

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
  (log/tracef "Building thunk to read column %s" i)
  (fn column-thunk []
    (let [rsmeta (.getMetaData rset)
          thunk  (read-column-thunk conn model rset rsmeta i)
          v      (thunk)]
      (next.jdbc.rs/read-column-by-index v rsmeta i))))

(defn- make-i->thunk [conn model rset]
  (comp (memoize (fn [i]
                   (make-column-thunk conn model rset i)))
        int))

(defn ^:no-doc make-cached-row-num->i->thunk
  "Returns a function that, given the current row number, returns a function that, given a column number, returns a cached
  thunk to fetch values of that column. Confusing, huh? Here's a chart to make it a bit easier to visualize:

  ```clj
  (make-cached-row-num->i->thunk conn model rset)
  =>
  (f current-row-number)
  =>
  (f column-index)
  =>
  (column-value-thunk)
  ```

  What's the point of this? The main point is to cache values that come out of the database, so we only fetch them once.
  This is used to implement our transient rows in [[toucan2.jdbc.result-set]] -- accessing the value of a transient row
  twice should not result in two calls to `.getObject`.

  The row number is used for cache-busting purposes, so we can reset the cache after each row is returned (so we don't
  accidentally cache values from the first row and return them for all the rows). The row number passed in here doesn't
  need to correspond to the actual row number from a JDBC standpoint; it's used only for cache-busting purposes."
  [conn model ^ResultSet rset]
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
              (log/tracef "Using cached value for column %s: %s" i cached-value)
              cached-value)
            (let [thunk (i->thunk i)
                  v     (thunk)]
              (swap! cached-values assoc i v)
              v)))))))

(defn ^:no-doc read-column-by-index-fn
  "Given a `java.sql.Connection` `conn`, a `model`, and a `java.sql.ResultSet` `rset`, return a function that can be used
  with [[next.jdbc.result-set/builder-adapter]]. The function has the signature

  ```clj
  (f builder rset i) => result
  ```

  When this function is called with a `next.jdbc` result set `builder`, a `java.sql.ResultSet` `rset`, and column
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
       (log/tracef "col %s => %s" i result)
       result))))
