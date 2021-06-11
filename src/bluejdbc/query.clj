(ns bluejdbc.query
  (:refer-clojure :exclude [compile])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.log :as log]
            [bluejdbc.realize :as realize]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

;; TODO -- should this be off by default?
(def ^:dynamic *include-queries-in-exceptions?* true)

(def ^:dynamic *call-count-thunk* (fn [])) ; no-op

(def ^:dynamic ^:private *return-compiled* false)

(def ^:dynamic ^:private *return-uncompiled* false)

(defn do-compiled [thunk]
  (binding [*return-compiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro compiled {:style/indent 0} [& body]
  `(do-compiled (fn [] ~@body)))

(defn do-uncompiled [thunk]
  (binding [*return-uncompiled* true]
    (u/do-returning-quit-early thunk)))

(defmacro uncompiled {:style/indent 0} [& body]
  `(do-uncompiled (fn [] ~@body)))

;; TODO -- this stuff should probably be part of `compile/compile`, not here.
(defn ^:deprecated compile [connectable tableable queryable options]
  (when *return-uncompiled*
    (throw (u/quit-early-exception queryable)))
  (let [compiled (compile/compile connectable tableable queryable options)]
    (when *return-compiled*
      (throw (u/quit-early-exception compiled)))
    compiled))

(m/defmulti reducible-query*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod reducible-query* :before :default
  [connectable tableable queryable options]
  (assert (some? connectable) "connectable should not be nil; use current-connectable to get the connectable to use")
  queryable)

(defn reducible-query
  ([queryable]
   (reducible-query nil nil queryable nil))

  ([connectable queryable]
   (reducible-query connectable nil queryable nil))

  ([connectable tableable queryable]
   (reducible-query connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (reducible-query* connectable tableable queryable options))))

(defn query
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (realize/realize (apply reducible-query args)))

(defn reduce-first
  ([reducible]
   (reduce-first identity reducible))

  ([xform reducible]
   (transduce
    (comp xform (take 1))
    (completing conj (comp first #_unreduced))
    []
    reducible)))

(defn query-one
  "Like `query`, but returns only the first row. Does not fetch additional rows regardless of whether the query would
  yielded them."
  {:arglists '([queryable]
               [connectable queryable]
               [connectable tableable queryable]
               [connectable tableable queryable options])}
  [& args]
  (reduce-first (map realize/realize) (apply reducible-query args)))

(m/defmulti execute!*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod execute!* :default
  [connectable tableable queryable options]
  (conn/with-connection [conn connectable tableable options]
    (let [reducible (reducible-query connectable tableable queryable options)]
      (if (:reducible? options)
        reducible
        (if (get-in options [:next.jdbc :return-keys])
          (realize/realize reducible)
          (let [update-count (reduce-first reducible)]
            (log/tracef "%d rows-affected." update-count)
            update-count))))))

(defn execute!
  "Compile and execute a `queryable` such as a String SQL statement, `[sql & params]` vector, or HoneySQL map. Intended
  for use with statements such as `UPDATE`, `INSERT`, or `DELETE`, or DDL statements like `CREATE TABLE`; for queries
  like `SELECT`, use `query` instead. Returns the number of rows affected, unless `{:next.jdbc {:return-keys true}}`
  is set, in which case it returns generated keys (see next.jdbc documentation for more details).

  You can return a reducible query instead by passing `:reducible?` in the options."
  ([queryable]                       (execute!  nil         nil       queryable nil))
  ([connectable queryable]           (execute!  connectable nil       queryable nil))
  ([connectable tableable queryable] (execute!  connectable tableable queryable nil))

  ([connectable tableable queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (execute!* connectable tableable queryable options))))

(defn do-with-call-counts
  "Impl for `with-call-count` macro; don't call this directly."
  [f]
  (let [call-count (atom 0)
        old-thunk  *call-count-thunk*]
    (binding [*call-count-thunk* #(do
                                    (old-thunk)
                                    (swap! call-count inc))]
      (f (fn [] @call-count)))))

(defmacro with-call-count
  "Execute `body`, trackingthe number of database queries and statements executed. This number can be fetched at any
  time withing `body` by calling function bound to `call-count-fn-binding`:

    (with-call-count [call-count]
      (select ...)
      (println \"CALLS:\" (call-count))
      (insert! ...)
      (println \"CALLS:\" (call-count)))
    ;; -> CALLS: 1
    ;; -> CALLS: 2"
  [[call-count-fn-binding] & body]
  `(do-with-call-counts (fn [~call-count-fn-binding] ~@body)))
