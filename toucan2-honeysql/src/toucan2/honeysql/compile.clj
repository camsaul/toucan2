(ns toucan2.honeysql.compile
  (:require [honeysql.core :as hsql]
            [honeysql.format :as hformat]
            [methodical.core :as m]
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
      (log/with-trace (format "Convert table identifier %s to table name with options %s"
                              (pr-str tableable)
                              (pr-str (:honeysql options)))
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
