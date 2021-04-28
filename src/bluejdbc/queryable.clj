(ns bluejdbc.queryable
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.util :as u]
            [methodical.core :as m]))

(m/defmulti queryable*
  {:arglists '([connectable tableable queryable options])}
  u/dispatch-on-first-three-args)

(m/defmethod queryable* :default
  [_ tableable queryable options]
  (throw (ex-info (format "Don't know how to convert %s to a query. Add an impl for queryable*" (pr-str queryable))
                  {:tableable tableable
                   :queryable queryable
                   :options   options})))

(m/defmethod queryable* [:default :default String]
  [_ _ s _]
  s)

(m/defmethod queryable* [:default :default clojure.lang.IPersistentMap]
  [_ _ m _]
  m)

(m/defmethod queryable* [:default :default clojure.lang.Sequential]
  [_ _ coll _]
  coll)

(defn queryable
  ([a-queryable]
   (queryable conn/*connectable* nil a-queryable nil))

  ([tableable a-queryable]
   (queryable conn/*connectable* tableable a-queryable nil))

  ([connectable tableable a-queryable]
   (queryable connectable tableable a-queryable nil))

  ([connectable tableable a-queryable options]
   (let [options (u/recursive-merge (conn/default-options connectable) options)]
     (queryable* connectable tableable a-queryable options))))

(defn queryable? [connectable tableable a-queryable]
  ;; TODO -- if we have a way to tell whether we would be using the default method or not, we wouldn't have to
  ;; actually invoke the method to figure out if something is queryable. (See
  ;; https://github.com/camsaul/methodical/issues/10)
  (boolean ((-> queryable*
                (m/add-primary-method :default (constantly false)))
            connectable tableable a-queryable nil)))
