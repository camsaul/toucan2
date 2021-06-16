(ns toucan2.tableable
  (:require [clojure.string :as str]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [toucan2.connectable.current :as conn.current]
            [toucan2.instance :as instance]
            [toucan2.util :as u]))

(m/defmulti table-name*
  {:arglists '([connectableᵈ tableableᵈᵗ options])}
  u/dispatch-on-first-two-args
  :combo (m.combo.threaded/threading-method-combination :second))

(m/defmethod table-name* [:default String]
  [_ s _]
  s)

(m/defmethod table-name* :default
  [connectable tableable _]
  (when-not (instance? clojure.lang.Named tableable)
    (throw (ex-info (format "Don't know how to convert %s to a table name. %s"
                            tableable (u/suggest-dispatch-values connectable tableable))
                    {:tableable tableable})))
  (if-let [nmspace (namespace tableable)]
    (last (str/split nmspace #"\."))
    (name tableable)))

(defn table-name
  ([tableable]                     (table-name* nil         tableable nil))
  ([connectable tableable]         (table-name* connectable tableable nil))
  ([connectable tableable options] (table-name* connectable tableable options)))

;; TODO -- shouldn't this take options as well?
(m/defmulti primary-key*
  "Return the primary key(s) for this table. Can be either a single keyword or a vector of multiple keywords for
  composite PKs."
  {:arglists '([connectableᵈ tableableᵈᵗ])}
  u/dispatch-on-first-two-args)

(m/defmethod primary-key* :default
  [_ _]
  :id)

(defn primary-key-keys
  "Return the primary key fields names, as a keywords, for a `tableable`. Always returns a sequence of keywords.

    (primary-key-keys :my-connection :user) ;-> [:id]"
  ([tableable]
   (let [connectable (conn.current/current-connectable tableable)]
     (primary-key-keys connectable tableable)))

  ([connectable tableable]
   (let [pk-keys (primary-key* connectable tableable)]
     (if (sequential? pk-keys)
       pk-keys
       [pk-keys]))))

(defn primary-key-values
  "Return the values of the primary key(s) of `obj` as a map.

    (primary-key-values a-user) ;-> {:id 1}"

  ([obj]
   (primary-key-values (instance/connectable obj) (instance/tableable obj) obj))

  ([connectable tableable m]
   (let [pk-keys (primary-key-keys connectable tableable)]
     (zipmap pk-keys (map m pk-keys)))))
