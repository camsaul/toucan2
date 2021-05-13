(ns bluejdbc.tableable
  (:require [bluejdbc.instance :as instance]
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

(defn primary-key-keys
  "Return the primary key fields names, as a keywords, for a `tableable`. Always returns a sequence of keywords.

    (primary-key-keys :my-connection :user) ;-> [:id]"
  [connectable tableable]
  (let [pk-keys (primary-key* connectable tableable)]
    (if (sequential? pk-keys)
      pk-keys
      [pk-keys])))

(defn primary-key-values
  "Return the values of the primary key(s) of `obj` as a map.

    (primary-key-values a-user) ;-> {:id 1}"

  ([obj]
   (primary-key-values (instance/connectable obj) (instance/table obj) obj))

  ([connectable tableable m]
   (let [pk-keys (primary-key-keys connectable tableable)]
     (zipmap pk-keys (map m pk-keys)))))
