(ns bluejdbc.test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            [bluejdbc.query :as query]
            [bluejdbc.test.load :as load]
            [clojure.test :refer :all]
            [environ.core :as env]
            [java-time :as t]
            [methodical.core :as m]))

#_(jdbc/defmethod jdbc/connection* :test-connection/h2
  [_ options]
  (jdbc/connection* "jdbc:h2:mem:bluejdbc_test;DB_CLOSE_DELAY=-1" options))

#_(jdbc/defmethod jdbc/connection* :test-connection/postgres
  [_ options]
  (jdbc/connection* "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam" options))

(def ^:private ^:dynamic *connection* nil)

(defn connection [& _]
  (or *connection*
      (env/env :jdbc-url)
      :test-connection/postgres))

(m/defmethod connection/db-type :test-connection/postgres
  [_]
  :postgresql)

(defn do-with-test-data [conn data thunk]
  (binding [query/*include-queries-in-exceptions*              true
            connection/*include-connection-info-in-exceptions* true]
    (try
      (load/create-database-with-test-data! conn data)
      (thunk)
      (finally
        (load/destroy-all-tables! conn data)))))

(defmacro with-test-data
  [[connectable data-source] & body]
  `(let [data#        (load/data ~data-source)
         connectable# ~connectable]
     (jdbc/with-connection [~'&conn connectable#]
       (do-with-test-data ~'&conn data# (fn [] ~@body)))))

(defonce ^:private test-connections* (atom nil))

(defn set-test-connections! [& connections]
  (reset! test-connections* connections))

(defn- test-connections []
  (or @test-connections*
      (filter
       some?
       (distinct
        [(env/env :jdbc-url-postgres)
         (env/env :jdbc-url-h2)
         (env/env :jdbc-url-sql-server)
         (env/env :jdbc-url-mysql)
         (env/env :jdbc-url)]))))

(println "Testing against:" (pr-str (test-connections)))

(defn do-with-every-test-connection [thunk]
  (doseq [conn (test-connections)]
    (binding [*connection* conn]
      (testing (format "connection = %s\n" (pr-str conn))
        (thunk)))))

(defmacro with-every-test-connection [& body]
  `(do-with-every-test-connection (fn [] ~@body)))

(deftest with-test-data-test
  (with-every-test-connection
    (with-test-data [(connection) :people]
      (is (= (case (connection/db-type &conn)
               :h2
               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T16:56-07:00")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T15:56-08:00")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T13:56-08:00")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T12:56-07:00")}]

               :postgresql
               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}])
             (jdbc/query (connection) "SELECT * FROM \"people\";"))))))
