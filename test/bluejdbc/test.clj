(ns bluejdbc.test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            [bluejdbc.integrations.postgres :as postgres]
            [bluejdbc.query :as query]
            [bluejdbc.test.load-test-data :as load]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

;; TODO -- load integrations automatically based on Connection type.
(comment postgres/keep-me)

(jdbc/defmethod jdbc/connection* :test-connection/h2
  [_ options]
  (jdbc/connection* "jdbc:h2:mem:bluejdbc_test;DB_CLOSE_DELAY=-1" options))

;; (jdbc/defmethod jdbc/named-connectable :postgres
;;   [_]
;;   "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam")

;; (jdbc/defmethod jdbc/named-connectable :mysql
;;   [_]
;;   "jdbc:mysql://localhost:3306/metabase_test?user=root")

(jdbc/defmethod jdbc/connection* :test-connection/postgres
  [_ options]
  (jdbc/connection* "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam" options))

(defn connection [& _] :test-connection/postgres)

(defn db-type [] :postgresql)

(defn ^:deprecated autoincrement-type []
  "SERIAL")

(defn ^:deprecated do-with-test-data [conn thunk]
  (jdbc/with-connection [conn conn]
    (try
      (jdbc/execute! conn "CREATE TABLE \"people\" (\"id\" INTEGER NOT NULL, \"name\" TEXT NOT NULL, \"created_at\" TIMESTAMP NOT NULL);")
      (t/with-clock (t/mock-clock (t/instant (t/offset-date-time "2020-04-21T16:56:00.000-07:00")) (t/zone-id "America/Los_Angeles"))
        (jdbc/insert! conn :people [:id :name :created_at]
                      [[1 "Cam" (t/offset-date-time "2020-04-21T16:56:00.000-07:00")]
                       [2 "Sam" (t/offset-date-time "2019-01-11T15:56:00.000-08:00")]
                       [3 "Pam" (t/offset-date-time "2020-01-01T13:56:00.000-08:00")]
                       [4 "Tam" (t/offset-date-time "2020-05-25T12:56:00.000-07:00")]]))
      (thunk)
      (finally
        (jdbc/execute! conn "DROP TABLE IF EXISTS \"people\";")))))

(defmacro ^:deprecated with-test-data
  "Execute `body` with some rows loaded into a `people` table."
  [conn & body]
  `(do-with-test-data ~conn (fn [] ~@body)))

(m/defmulti db-type*
  {:arglists '([connectable])}
  u/dispatch-on-first-arg)

(m/defmethod db-type* :test-connection/postgres
  [_]
  :postgresql)

(when-let [klass (try
                   (Class/forName "org.postgresql.jdbc.PgConnection")
                   (catch Throwable _))]
  (m/defmethod db-type* klass
    [_]
    :postgresql))

(defn ^:deprecated db-type
  ([]
   (db-type* (connection)))
  ([connectable]
   (db-type* connectable)))

(defn do-with-test-data-2 [db-type connectable data-source thunk]
  (let [data (load/data data-source)]
    (jdbc/with-connection [conn connectable]
      (binding [query/*include-queries-in-exceptions*              true
                connection/*include-connection-info-in-exceptions* true]
        (try
          (load/create-database-with-test-data! db-type conn data)
          (thunk)
          (finally
            (load/destroy-all-tables! db-type conn data)))))))

(defmacro with-test-data-2
  [[connectable data-source] & body]
  `(let [connectable# ~connectable]
     (do-with-test-data-2 (db-type connectable#) connectable# ~data-source (fn [] ~@body))))

(deftest with-test-data-2-test
  (with-test-data-2 [(connection) :people]
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           (jdbc/query (connection) "SELECT * FROM people;" {:connection/type (db-type)})))))
