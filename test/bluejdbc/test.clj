(ns bluejdbc.test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            [bluejdbc.query :as query]
            [bluejdbc.test.load-test-data :as load]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

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

(m/defmethod connection/db-type :test-connection/postgres
  [_]
  :postgresql)

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

(defn do-with-test-data [connectable data-source thunk]
  (let [data (load/data data-source)]
    (jdbc/with-connection [conn connectable]
      (binding [query/*include-queries-in-exceptions*              true
                connection/*include-connection-info-in-exceptions* true]
        (try
          (load/create-database-with-test-data! conn data)
          (thunk)
          (finally
            (load/destroy-all-tables! conn data)))))))

(defmacro with-test-data
  [[connectable data-source] & body]
  `(let [connectable# ~connectable]
     (do-with-test-data connectable# ~data-source (fn [] ~@body))))

(deftest with-test-data-test
  (with-test-data [(connection) :people]
    (is (= [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
            {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
            {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
            {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}]
           (jdbc/query (connection) "SELECT * FROM people;")))))
