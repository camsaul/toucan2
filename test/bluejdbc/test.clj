(ns bluejdbc.test
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            bluejdbc.integrations.postgresql
            [bluejdbc.query :as query]
            [bluejdbc.test.load :as load]
            [clojure.test :refer :all]
            [environ.core :as env]
            [java-time :as t]))

(comment bluejdbc.integrations.postgresql/keep-me)

(derive :test-connection/h2       ::connection)
(derive :test-connection/postgres ::connection)
(derive :test-connection/postgres :bluejdbc.integrations/postgres)

(jdbc/defmethod jdbc/connection* :test-connection/h2
  [_ options]
  (jdbc/connection* (env/env :jdbc-url-h2 "jdbc:h2:mem:bluejdbc_test;DB_CLOSE_DELAY=-1") options))

(jdbc/defmethod jdbc/connection* :test-connection/postgres
  [_ options]
  (jdbc/connection* (env/env :jdbc-url-postgres "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam") options))

(defn do-with-test-data [connectable data thunk]
  (binding [query/*include-queries-in-exceptions*              true
            connection/*include-connection-info-in-exceptions* true]
    (try
      (load/create-database-with-test-data! connectable data)
      (thunk)
      (finally
        (load/destroy-all-tables! connectable data)))))

(defmacro with-test-data
  [[connectable data-source] & body]
  `(let [data#        (load/data ~data-source)
         connectable# ~connectable]
     (jdbc/with-connection [~'&conn connectable#]
       (do-with-test-data ~'&conn data# (fn [] ~@body)))))

(defonce ^:private test-connectables* (atom nil))

(defn set-test-connectables! [& connections]
  (reset! test-connectables* connections))

(defn- test-connectables []
  (or @test-connectables*
      (descendants ::connection)))

(println "Testing against:" (pr-str (test-connectables)))

(defn do-with-every-test-connectable [f]
  (doseq [connectable (test-connectables)]
    (testing (format "connection = %s\n" (pr-str connectable))
      (f connectable))))

(defmacro with-every-test-connectable [[connectable-binding] & body]
  `(do-with-every-test-connectable (fn [~connectable-binding] ~@body)))

(deftest with-test-data-test
  (with-every-test-connectable [connectable]
    (with-test-data [connectable :people]
      (is (= (case (keyword (name connectable))
               :h2
               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T16:56-07:00")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T15:56-08:00")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T13:56-08:00")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T12:56-07:00")}]

               :postgres
               [{:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56Z")}
                {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56Z")}
                {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56Z")}
                {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56Z")}])
             (jdbc/query-all connectable "SELECT * FROM \"people\";"))))))
