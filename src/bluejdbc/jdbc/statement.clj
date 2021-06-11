(ns bluejdbc.jdbc.statement
  (:require [bluejdbc.jdbc.result-set :as rs]
            [bluejdbc.log :as log]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [methodical.impl.combo.threaded :as m.combo.threaded]
            [next.jdbc :as next.jdbc]
            [next.jdbc.prepare :as next.jdbc.prepare]
            [potemkin :as p]
            [pretty.core :as pretty]))

(m/defmulti set-parameter!*
  {:arglists '([connectable tableable x ^java.sql.PreparedStatement stmt ^Long i options])}
  u/dispatch-on-first-three-args
  :combo (m.combo.threaded/threading-method-combination :third))

(m/defmethod set-parameter!* :default
  [_ _ x ^java.sql.PreparedStatement stmt ^Long i _]
  (log/tracef "Set parameter %d -> %s %s" i (some-> x class (.getCanonicalName)) (pr-str x))
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

(p/deftype+ ReducibleStatement [connectable tableable ^java.sql.PreparedStatement stmt options]
  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (letfn [(try-execute [thunk]
              (try
                (thunk)
                (catch Throwable e
                  (throw (ex-info (format "Error executing query: %s" (ex-message e))
                                  {:statement stmt, :options options}
                                  e)))))]
      (if (get-in options [:next.jdbc :return-keys])
        (do
          (try-execute #(.executeUpdate stmt))
          (with-open [rs (.getGeneratedKeys stmt)]
            (reduce rf init (rs/reducible-result-set connectable tableable rs options))))
        (let [has-result-set? (try-execute #(.execute stmt))]
          (if has-result-set?
            (with-open [rs (.getResultSet stmt)]
              (reduce rf init (rs/reducible-result-set connectable tableable rs options)))
            ;; TODO -- should this be reduced with rf and init??
            #_(reduce rf init (reduced [(.getUpdateCount stmt)]))
            [(.getUpdateCount stmt)])))))

  pretty/PrettyPrintable
  (pretty [_]
    (list (pretty/qualify-symbol-for-*ns* `reducible-statement) connectable tableable stmt options)))

(defn reducible-statement [connectable tableable stmt options]
  (->ReducibleStatement connectable tableable stmt options))

(defn prepare ^java.sql.PreparedStatement [connectable tableable conn [sql & params] options]
  (let [params     (for [param params]
                     (parameter connectable tableable param options))
        sql-params (cons sql params)]
    (next.jdbc/prepare conn sql-params (:next.jdbc options))))
