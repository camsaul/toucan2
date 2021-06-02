(ns bluejdbc.statement
  (:require [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [next.jdbc.prepare :as next.jdbc.prepare]
            [potemkin :as p]
            [pretty.core :as pretty]))

(m/defmulti set-parameter!*
  {:arglists '([connectable tableable x ^java.sql.PreparedStatement stmt ^Long i options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod set-parameter!* :default
  [_ _ x ^java.sql.PreparedStatement stmt ^Long i _]
  (log/tracef "Set parameter %d -> %s %s" i (some-> x class (.getCanonicalName)) x)
  (next.jdbc.prepare/set-parameter x stmt i))

(p/deftype+ Parameter [connectable tableable x options]
  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `parameter) connectable tableable x options))

  next.jdbc.prepare/SettableParameter
  (set-parameter [_ stmt i]
    (set-parameter!* connectable tableable x stmt i options)))

(defn parameter [connectable tableable x options]
  (->Parameter connectable tableable x options))
