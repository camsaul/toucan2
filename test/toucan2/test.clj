(ns toucan2.test
  (:require
   [clojure.string :as str]
   [methodical.core :as m]
   [pjstadig.humane-test-output :as humane-test-output]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [camel-snake-kebab.core :as csk]))

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

(defmulti ^:private default-test-db-url
  {:arglists '([db-type])}
  keyword)

(defmethod default-test-db-url :postgres
  [_db-type]
  "jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam")

(defmethod default-test-db-url :h2
  [_db-type]
  "jdbc:h2:mem:toucan2")

(defn- test-db-url [db-type]
  (let [env-var (format "JDBC_URL_%s" (str/upper-case (name db-type)))]
    (or (System/getenv env-var)
        (default-test-db-url db-type))))

(m/defmulti create-table-sql-file
  {:arglists '([db-type model-or-table-name])}
  (fn [db-type model-or-table-name]
    [(keyword db-type) (keyword model-or-table-name)]))

(m/defmethod create-table-sql-file [:default :people]
  [_db-type _table-name]
  "test/toucan2/test/people.sql")

(m/defmethod create-table-sql-file [:default :venues]
  [_db-type _table-name]
  "test/toucan2/test/venues.sql")

(defn- create-table-statements [db-type table-name]
  (for [stmt (str/split (str/trim (slurp (create-table-sql-file db-type table-name))) #";")]
    (str/trim stmt)))

(defn create-table! [db-type ^java.sql.Connection conn table-name]
  (try
    #_(let [start-time-ms (System/currentTimeMillis)])
    (doseq [^String sql (create-table-statements db-type table-name)]
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

(def ^:private initialized? (atom #{}))

(defn- set-up-test-db! [db-type]
  (when-not (contains? @initialized? db-type)
    (println "Set up" db-type "test DB")
    (with-open [conn (java.sql.DriverManager/getConnection (test-db-url db-type))]
      (doseq [table-name [:people
                          :venues]]
        (create-table! db-type conn table-name)))
    (swap! initialized? conj db-type)))

(m/defmethod conn/do-with-connection ::h2
  [_connectable f]
  (set-up-test-db! :h2)
  (conn/do-with-connection (test-db-url :h2) f))

(m/defmethod conn/do-with-connection ::postgres
  [_connectable f]
  (set-up-test-db! :postgres)
  (conn/do-with-connection (test-db-url :postgres) f))

(def ^:dynamic *db-type*
  (or (some-> (System/getenv "DB_TYPE") str/lower-case keyword)
      :postgres))

(m/defmethod conn/do-with-connection ::db
  [_connectable f]
  (let [db-type (keyword "toucan2.test" (name *db-type*))]
    (assert (not= db-type ::db))
    (conn/do-with-connection db-type f)))

(defn do-with-discarded-table-changes [db-type table-name thunk]
  (try
    (thunk)
    (finally
      (with-open [conn (java.sql.DriverManager/getConnection (test-db-url db-type))]
        (create-table! db-type conn table-name)))))

(defmacro with-discarded-table-changes
  {:style/indent 1}
  [table-name & body]
  `(do-with-discarded-table-changes *db-type* ~table-name (^:once fn* [] ~@body)))

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
