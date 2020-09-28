(ns bluejdbc.test
  "Test utils."
  (:require [bluejdbc.core :as jdbc]
            [clojure.string :as str]
            [environ.core :as env]
            [java-time :as t]))

(defn jdbc-url
  "JDBC URL to run tests against."
  ^String []
  (let [url (env/env :jdbc-url)]
    (assert (not (str/blank? url)) "Env var JDBC_URL msut be set before running tests")
    url))

(defn db-type
  "Type of database we're tetsing against, e.g. `:postgresql`."
  []
  (keyword (second (re-find #"^jdbc:([^:]+):" (jdbc-url)))))

(defn as-set [dbs]
  (if (keyword? dbs) #{dbs} (set dbs)))

(defmacro only
  "Only run `body` against DBs if we are currently testing against them.

    (only :postgresql
      ...)

    (only #{:mysql :mariadb}
      ...)"
  {:style/indent 1}
  [dbs & body]
  `(when (contains? (as-set ~dbs) (db-type))
     ~@body))

(defmacro exclude
  "Execute `body` for all DBs except `dbs`."
  {:style/indent 1}
  [dbs & body]
  `(when-not (contains? (as-set ~dbs) (db-type))
     ~@body))

(defn do-with-test-data [conn thunk]
  (jdbc/with-connection [conn conn]
    (try
      (jdbc/execute! conn "CREATE TABLE people (id INTEGER NOT NULL, name TEXT NOT NULL, created_at TIMESTAMP NOT NULL);")
      (t/with-clock (t/mock-clock (t/instant (t/offset-date-time "2020-04-21T16:56:00.000-07:00")) (t/zone-id "America/Los_Angeles"))
        (jdbc/insert! conn :people [:id :name :created_at]
                      [[1 "Cam" (t/offset-date-time "2020-04-21T16:56:00.000-07:00")]
                       [2 "Sam" (t/offset-date-time "2019-01-11T15:56:00.000-08:00")]
                       [3 "Pam" (t/offset-date-time "2020-01-01T13:56:00.000-08:00")]
                       [4 "Tam" (t/offset-date-time "2020-05-25T12:56:00.000-07:00")]]))
      (thunk)
      (finally
        (jdbc/execute! conn "DROP TABLE IF EXISTS people;")))))

(defmacro with-test-data
  "Execute `body` with some rows loaded into a `people` table."
  [conn & body]
  `(do-with-test-data ~conn (fn [] ~@body)))
