(ns bluejdbc.test
  (:require [bluejdbc.connectable :as connectable]
            [methodical.core :as m]))

(m/defmethod connectable/connection* :test/postgres
  [_ options]
  (next-method "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam" (merge {:default-options true} options)))

(defn- table-names [connectable]
  (connectable/with-connection [[conn] :test/postgres]
    (with-open [rs (.getTables (.getMetaData conn) nil nil nil (into-array String ["TABLE"]))]
      (into #{} (take-while some? (repeatedly (fn []
                                                (when (.next rs)
                                                  (.getString rs "TABLE_NAME")))))))))

(defn- has-test-data? [connectable]
  (contains? (table-names connectable) "people"))

(defn- load-test-data-if-needed! [connectable]
  (when-not (has-test-data? connectable)
    (connectable/with-connection [[conn] connectable]
      (with-open [stmt (.createStatement conn)]
        (doseq [sql ["CREATE TABLE IF NOT EXISTS people (id serial PRIMARY KEY NOT NULL, name text, created_at timestamp with time zone);"
                     (str "INSERT INTO people (id, name, created_at) "
                          "VALUES "
                          "(1, 'Cam', '2020-04-21T16:56:00.000-07:00'::timestamptz), "
                          "(2, 'Sam', '2019-01-11T15:56:00.000-08:00'::timestamptz), "
                          "(3, 'Pam', '2020-01-01T13:56:00.000-08:00'::timestamptz), "
                          "(4, 'Tam', '2020-05-25T12:56:00.000-07:00'::timestamptz)")]]
          (.execute stmt sql))))))

(defn do-with-test-data [thunk]
  (load-test-data-if-needed! :test/postgres)
  (thunk))
