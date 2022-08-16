(ns toucan2.test
  (:require
   [clojure.string :as str]
   [methodical.core :as m]
   [pjstadig.humane-test-output :as humane-test-output]
   [toucan2.connection :as conn]
   [toucan2.model :as model]))

(set! *warn-on-reflection* true)

(humane-test-output/activate!)

(defmethod print-dup java.time.LocalDateTime
  [t writer]
  (print-dup (list 'java.time.LocalDateTime/parse (str t))
             writer))

(defmethod print-method java.time.LocalDateTime
  [t writer]
  (print-method (list 'java.time.LocalDateTime/parse (str t))
                writer))

(defn- test-db-url []
  (or (System/getenv "JDBC_URL_POSTGRES")
      "jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam"))

(m/defmulti create-table-sql-file
  {:arglists '([table-name])}
  keyword)

(m/defmethod create-table-sql-file :people
  [_table-name]
  "test/toucan2/test/people.sql")

(m/defmethod create-table-sql-file :venues
  [_table-name]
  "test/toucan2/test/venues.sql")

(defn- create-table-statements [table-name]
  (for [stmt (str/split (str/trim (slurp (create-table-sql-file table-name))) #";")]
    (str/trim stmt)))

(defn create-table! [^java.sql.Connection conn table-name]
  (try
    #_(let [start-time-ms (System/currentTimeMillis)])
    (doseq [^String sql (create-table-statements table-name)]
      #_(println sql)
      (with-open [stmt (.createStatement conn)]
        (try
          (.execute stmt sql)
          (catch Throwable e
            (throw (ex-info (format "Error executing SQL: %s" (ex-message e))
                            {:sql sql}
                            e))))))
    #_(printf "✔ done in %d ms\n\n" (- (System/currentTimeMillis) start-time-ms))
    (catch Throwable e
      (throw (ex-info (format "Error creating table %s: %s" table-name (ex-message e))
                      {:table table-name}
                      e)))))

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

(def ^:private initialized? (atom false))

(defn- set-up-test-db! []
  (when-not @initialized?
    (with-open [conn (java.sql.DriverManager/getConnection (test-db-url))]
      (doseq [table-name [:people
                          :venues]]
        (create-table! conn table-name)))
    (reset! initialized? true)))

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
