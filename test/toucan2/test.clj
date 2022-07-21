(ns toucan2.test
  (:require
   [clojure.string :as str]
   [methodical.core :as m]
   [toucan2.connection :as conn]))

(defn- test-db-url []
  (or (System/getenv "JDBC_URL_POSTGRES")
      "jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam"))

(def ^:private test-data-statements
  (into []
        (map (partial str/join \newline))
        [ ;; people table
         ["DROP TABLE IF EXISTS people;"]
         ["CREATE TABLE people ("
          "  id serial PRIMARY KEY NOT NULL,"
          "  name text,"
          "  created_at timestamp with time zone"
          ");"]
         ["INSERT INTO people (id, name, created_at)"
          "VALUES"
          "(1, 'Cam', '2020-04-21T16:56:00.000-07:00'::timestamptz),"
          "(2, 'Sam', '2019-01-11T15:56:00.000-08:00'::timestamptz),"
          "(3, 'Pam', '2020-01-01T13:56:00.000-08:00'::timestamptz),"
          "(4, 'Tam', '2020-05-25T12:56:00.000-07:00'::timestamptz);"]
         ;; venues table
         ["DROP TABLE IF EXISTS venues;"]
         ["CREATE TABLE venues ("
          "  id SERIAL PRIMARY KEY,"
          "  name VARCHAR(256) UNIQUE NOT NULL,"
          "  category VARCHAR(256) NOT NULL,"
          "  created_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz,"
          "  updated_at TIMESTAMP NOT NULL DEFAULT '2017-01-01T00:00:00Z'::timestamptz"
          ");"]
         ["INSERT INTO venues (name, category)"
          "VALUES"
          "('Tempest', 'bar'),"
          "('Ho''s Tavern', 'bar'),"
          "('BevMo', 'store');"]
         ]))
(defonce ^:private ^{:arglists '([])} set-up-test-db!
  (let [dlay (delay
               (with-open [conn (java.sql.DriverManager/getConnection (test-db-url))]
                 (doseq [^String sql test-data-statements]
                   (println sql)
                   (let [start-time-ms (System/currentTimeMillis)]
                     (with-open [stmt (.createStatement conn)]
                       (.execute stmt sql))
                     (printf "✔ done in %d ms\n\n" (- (System/currentTimeMillis) start-time-ms))))))]
    (fn []
      @dlay
      :done)))

(m/defmethod conn/do-with-connection ::db
  [_connectable f]
  (set-up-test-db!)
  (conn/do-with-connection (test-db-url) f))
