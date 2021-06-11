(ns bluejdbc.test
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.connectable.current :as conn.current]
            bluejdbc.integrations.postgresql
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]
            [environ.core :as env]
            [methodical.core :as m]))

(comment bluejdbc.integrations.postgresql/keep-me)

(derive :test/postgres :bluejdbc.jdbc/postgresql)

(def test-postgres-url
  (env/env :jdbc-url-postgres "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam"))

(m/defmethod conn/connection* :test/postgres
  [_ options]
  (next-method
   test-postgres-url
   options))

(derive :test/postgres-with-quoting :test/postgres)

(m/defmethod conn.current/default-options-for-connectable* :test/postgres-with-quoting
  [_]
  {:honeysql {:quoting :ansi}})

(defn- table-names [connectable]
  (conn/with-connection [conn :test/postgres]
    (with-open [rs (.getTables (.getMetaData conn) nil nil nil (into-array String ["TABLE"]))]
      (into #{} (take-while some? (repeatedly (fn []
                                                (when (.next rs)
                                                  (.getString rs "TABLE_NAME")))))))))

(defn- has-test-data? [connectable table-name]
  (contains? (table-names connectable) (name table-name)))

(m/defmulti ^:private load-test-data-if-needed!
  {:arglists '([connectableᵈ table-nameᵈᵗ])}
  u/dispatch-on-first-two-args)

(m/defmethod load-test-data-if-needed! :around :default
  [connectable table-name]
  (when-not (has-test-data? connectable table-name)
    (log/tracef "creating %s table for %s" table-name connectable)
    (conn/with-connection [conn connectable]
      (with-open [stmt (.createStatement conn)]
        (doseq [sql (next-method connectable table-name)]
          (.execute stmt sql))))))

(m/defmethod load-test-data-if-needed! [:default :people]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS people (id serial PRIMARY KEY NOT NULL, name text, created_at timestamp with time zone);"
   "INSERT INTO people (id, name, created_at)
    VALUES
    (1, 'Cam', '2020-04-21T16:56:00.000-07:00'::timestamptz),
    (2, 'Sam', '2019-01-11T15:56:00.000-08:00'::timestamptz),
    (3, 'Pam', '2020-01-01T13:56:00.000-08:00'::timestamptz),
    (4, 'Tam', '2020-05-25T12:56:00.000-07:00'::timestamptz)"])

(m/defmethod load-test-data-if-needed! [:default :venues]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS venues (
     id SERIAL PRIMARY KEY,
     name VARCHAR(256) UNIQUE NOT NULL,
     category VARCHAR(256) NOT NULL,
     created_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz,
     updated_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz
    );"
   "INSERT INTO venues (name, category)
    VALUES
    ('Tempest', 'bar'),
    ('Ho''s Tavern', 'bar'),
    ('BevMo', 'store')"])

(defn do-with-test-data [thunk]
  (doseq [table [:people :venues]]
    (load-test-data-if-needed! :test/postgres table))
  (thunk))

(defn do-with-default-connection [thunk]
  (try
    (m/add-primary-method! conn/connection* :bluejdbc/default (fn [_ _ options]
                                                                (conn/connection* :test/postgres options)))
    (derive :bluejdbc/default :bluejdbc.jdbc/postgresql)
    (testing "using default connection"
      (thunk))
    (finally
      (underive :bluejdbc/default :bluejdbc.jdbc/postgresql)
      (m/remove-primary-method! conn/connection* :bluejdbc/default))))

(defmacro with-default-connection [& body]
  `(do-with-default-connection (fn [] ~@body)))

(defn do-with-venues-reset [thunk]
  (try
    (thunk)
    (finally
      (query/execute! :test/postgres "DROP TABLE IF EXISTS venues;")
      (load-test-data-if-needed! :test/postgres :venues))))

(defmacro with-venues-reset [& body]
  `(do-with-venues-reset (fn [] ~@body)))
