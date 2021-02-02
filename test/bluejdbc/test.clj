(ns bluejdbc.test
  (:require [bluejdbc.core :as jdbc]
            [clojure.test :refer :all]
            [java-time :as t]))

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

(defn db-type [] :postgres)

(defn autoincrement-type []
  "SERIAL")

(defn do-with-test-data [conn thunk]
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

(defmacro with-test-data
  "Execute `body` with some rows loaded into a `people` table."
  [conn & body]
  `(do-with-test-data ~conn (fn [] ~@body)))

(defmacro with-test-table {:style/indent 3} [conn table-name fields & body]
  `(let [conn# ~conn]
     (try
       (jdbc/execute! conn# ~(format "DROP TABLE IF EXISTS %s;" (name table-name)))
       (is (= 0
              (jdbc/execute! conn# (format "CREATE TABLE %s %s;" ~(name table-name) ~fields))))
       ~@body
       (finally
         (jdbc/execute! conn# ~(format "DROP TABLE IF EXISTS %s;" (name table-name)))))))
