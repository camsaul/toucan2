(ns toucan2.test
  (:require
   [clojure.string :as str]
   [methodical.core :as m]
   [pjstadig.humane-test-output :as humane-test-output]
   [toucan2.connection :as conn]
   [toucan2.model :as model]))

(set! *warn-on-reflection* true)

(humane-test-output/activate!)

(defn- test-db-url []
  (or (System/getenv "JDBC_URL_POSTGRES")
      "jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam"))

(m/defmulti create-table-statements
  {:arglists '([table-name])}
  keyword)

(m/defmethod create-table-statements :after :default
  [statements]
  (into [] (map (partial str/join \newline)) statements))

(m/defmethod create-table-statements :people
  [_table-name]
  [["DROP TABLE IF EXISTS people;"]
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
    "(4, 'Tam', '2020-05-25T12:56:00.000-07:00'::timestamptz);"]])

(m/defmethod create-table-statements :venues
  [_table-name]
  [["DROP TABLE IF EXISTS venues;"]
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
    "('BevMo', 'store');"]])

(defn- create-table! [^java.sql.Connection conn table-name]
  (let [#_start-time-ms #_(System/currentTimeMillis)]
    (doseq [^String sql (create-table-statements table-name)]
      #_(println sql)
      (with-open [stmt (.createStatement conn)]
        (.execute stmt sql)))
    #_(printf "✔ done in %d ms\n\n" (- (System/currentTimeMillis) start-time-ms))))

(defn do-with-discarded-table-changes [table-name thunk]
  (try
    (thunk)
    (finally
      (with-open [conn (java.sql.DriverManager/getConnection (test-db-url))]
        (create-table! conn table-name)))))

(defmacro with-discarded-table-changes
  {:style/indent 1}
  [table-name & body]
  `(do-with-discarded-table-changes ~table-name (^:once fn* [] ~@body)))

(defonce ^:private ^{:arglists '([])} set-up-test-db!
  (let [dlay (delay
               (with-open [conn (java.sql.DriverManager/getConnection (test-db-url))]
                 (doseq [table-name (keys (m/primary-methods create-table-statements))]
                   (create-table! conn table-name))))]
    (fn []
      @dlay
      :done)))

(m/defmethod conn/do-with-connection ::db
  [_connectable f]
  (set-up-test-db!)
  (conn/do-with-connection (test-db-url) f))

(m/defmethod model/default-connectable ::models
  [_model]
  ::db)

(derive ::people ::models)

(m/defmethod model/table-name ::people
  [_model]
  "people")

(derive ::venues ::models)

(m/defmethod model/table-name ::venues
  [_model]
  "venues")
