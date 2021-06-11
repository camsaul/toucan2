(ns bluejdbc.queryable
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]))

(m/defmulti queryable*
  {:arglists '([connectableᵈ tableableᵈ queryableᵈᵗ options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod queryable* :default
  [connectable tableable queryable options]
  (throw (ex-info (format "Don't know how to convert %s to a query. %s"
                          queryable
                          (u/suggest-dispatch-values connectable tableable queryable))
                  {:tableable tableable
                   :queryable queryable
                   :options   options})))

;; derive from `:bluejdbc/queryable` to have something be considered queryable out of the box without having to
;; implement any additional methods.

(derive String :bluejdbc/queryable)
(derive clojure.lang.IPersistentMap :bluejdbc/queryable)
(derive clojure.lang.Sequential :bluejdbc/queryable)

(m/defmethod queryable* [:default :default :bluejdbc/queryable]
  [_ _ x _]
  x)

(defn queryable
  ([a-queryable]                       (queryable nil         nil       a-queryable nil))
  ([tableable a-queryable]             (queryable nil         tableable a-queryable nil))
  ([connectable tableable a-queryable] (queryable connectable tableable a-queryable nil))

  ([connectable tableable a-queryable options]
   (let [[connectable options] (conn.current/ensure-connectable connectable tableable options)]
     (queryable* connectable tableable a-queryable options))))

(defn queryable? [connectable tableable a-queryable]
  ;; TODO -- if we have a way to tell whether we would be using the default method or not, we wouldn't have to
  ;; actually invoke the method to figure out if something is queryable. (See
  ;; https://github.com/camsaul/methodical/issues/10)
  (boolean ((-> queryable*
                (m/add-primary-method :default (constantly false)))
            connectable tableable a-queryable nil)))
