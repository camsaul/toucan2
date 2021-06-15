(ns toucan2.honeysql.compile
  (:require [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [potemkin :as p]
            [pretty.core :as pretty]
            [toucan2.compile :as compile]
            [toucan2.log :as log]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

;; TODO -- I'm not 100% sure what the best way to pass conn/options information to HoneySQL stuff like
;; TableIdentifier is. Should we just use whatever is in place when we compile the HoneySQL form? Should a Table
;; identifier get its own options?
(def ^:dynamic *compile-connectable* nil)
(def ^:dynamic *compile-options* nil)

;; TODO -- should `connectable` be an arg here too?
(p/defrecord+ TableIdentifier [tableable options]
  pretty/PrettyPrintable
  (pretty [_]
    (list* (pretty/qualify-symbol-for-*ns* `table-identifier) tableable (when options [options])))

  hformat/ToSql
  (to-sql [_]
    (let [options (u/recursive-merge *compile-options* options)]
      (log/with-trace ["Convert table identifier %s to table name with options %s"
                       tableable
                       (:honeysql options)]
        (-> (tableable/table-name *compile-connectable* tableable options)
            (hsql/quote-identifier :style (get-in options [:honeysql :quoting])))))))

(defn table-identifier
  ([tableable]         (->TableIdentifier tableable nil))
  ([tableable options] (->TableIdentifier tableable options)))

(defn table-identifier? [x]
  (instance? TableIdentifier x))

(m/defmethod compile/compile* [:default :default :toucan2/honeysql]
  [connectable _ honeysql-form {options :honeysql}]
  (assert (not (contains? honeysql-form :next.jdbc))
          (format "Options should not be present in honeysql-form! Got: %s" (pr-str honeysql-form)))
  (binding [*compile-connectable* connectable
            *compile-options*     options]
    (apply hsql/format honeysql-form (mapcat identity options))))

(m/defmulti to-sql*
  {:arglists '([connectableᵈ tableableᵈ columnᵈ valueᵈᵗ options])}
  u/dispatch-on-first-four-args
  :combo (m.combo.threaded/threading-method-combination :fourth))

(m/defmethod to-sql* :around :default
  [connectable tableable column v options]
  (let [result (next-method connectable tableable column v options)]
    (if (sequential? result)
      (let [[s & params] result]
        (assert (string? s) (format "to-sql* should return either string or [string & parms], got %s" (pr-str result)))
        (doseq [param params]
          (hformat/add-anon-param param))
        s)
      result)))

(p/defrecord+ Value [connectable tableable column v options]
  pretty/PrettyPrintable
  (pretty [_]
    (list* (pretty/qualify-symbol-for-*ns* `value) connectable tableable column v (when options [options])))

  hformat/ToSql
  (to-sql [this]
    (log/with-trace ["Convert %s to SQL" this]
      (to-sql* connectable tableable column v options))))

(defn value
  ([connectable tableable column v]
   (value connectable tableable column v nil))

  ([connectable tableable column v options]
   (assert (keyword? column) (format "column should be a keyword, got %s" (pr-str column)))
   (when (seq options)
     (assert (map? options) (format "options should be a map, got %s" (pr-str options))))
   (->Value connectable tableable column v options)))

(defn maybe-wrap-value
  "If there's an applicable impl of `to-sql*` for connectable + tableable + column + (class v), wrap in `v` in a
  `Value`; if there's not one, return `v` as-is.

  DEPRECATED -- this is really HoneySQL-specific and as such shouldn't be here"
  ([connectable tableable column v]
   (maybe-wrap-value connectable tableable column v nil))

  ([connectable tableable column v options]
   (let [dv (m/dispatch-value to-sql* connectable tableable column v)]
     (if (m/effective-primary-method to-sql* dv)
       (log/with-trace ["Found to-sql* method impl for dispatch value %s; wrapping in Value" dv]
         (value connectable tableable column v options))
       v))))
