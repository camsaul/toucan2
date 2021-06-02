(ns bluejdbc.connectable.current
  (:require [bluejdbc.util :as u]
            [methodical.core :as m]))

;; The only reason these are in their own namespace is to prevent circular refs.

(def ^:dynamic *current-connectable*
  "The current connectable bound by `with-connection`, if there is one; you can also bind this yourself to define the
  connectable that should be used when no explicit connectable is specified.

  Don't use this value directly; instead, call `(current-connectable)` which will fall back to `:bluejdbc/default` if
  nothing is bound."
  nil)

(def ^:dynamic ^java.sql.Connection *current-connection* nil)

(m/defmulti default-connectable-for-tableable*
  {:arglists '([tableable options])}
  u/dispatch-on-first-arg)

(m/defmethod default-connectable-for-tableable* :default
  [_ _]
  :bluejdbc/default)

(defn current-connectable
  "Return the connectable that should be used if no explicit connectable is specified:

  * If current connectable (`conn.current/*current-connectable*`) if bound, returns that

  * If `tableable` was passed, returns `default-connectable-for-tableable*` (by default `:bluejdbc/default`)

  * Otherwise returns `:bluejdbc/default`."
  ([]
   (current-connectable nil nil))

  ([tableable]
   (current-connectable tableable nil))

  ([tableable options]
   {:post [(some? %)]}
   (or *current-connectable*
       (default-connectable-for-tableable* tableable options))))

(m/defmulti default-options*
  {:arglists '([connectable])}
  u/dispatch-on-first-arg)

(defn ensure-connectable [connectable tableable options]
  (let [connectable (or connectable (current-connectable tableable options))
        options     (u/recursive-merge (default-options* connectable) options)]
    [connectable options]))
