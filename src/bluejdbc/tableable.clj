(ns bluejdbc.tableable
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti table-name*
  {:arglists '([connectable tableable options])}
  u/dispatch-on-first-two-args)

(m/defmethod table-name* [:default String]
  [_ s _]
  s)

(m/defmethod table-name* :default
  [_ tableable _]
  (when-not (instance? clojure.lang.Named tableable)
    (throw (ex-info (format "Don't know how to convert %s to a table name" (pr-str tableable))
                    {:tableable tableable})))
  (if-let [nmspace (namespace tableable)]
    nmspace
    (name tableable)))

(defn table-name
  ([tableable]                     (table-name* :current    tableable nil))
  ([connectable tableable]         (table-name* connectable tableable nil))
  ([connectable tableable options] (table-name* connectable tableable options)))

;; TODO -- shouldn't this take options as well?
(m/defmulti primary-key*
  {:arglists '([connectable tableable])}
  u/dispatch-on-first-two-args)

(m/defmethod primary-key* :default
  [_ _]
  :id)

(defn primary-key
  ([tableable]             (primary-key* conn/*connectable* tableable))
  ([connectable tableable] (primary-key* connectable        tableable)))
