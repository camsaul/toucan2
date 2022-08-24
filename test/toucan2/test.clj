(ns toucan2.test
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [pjstadig.humane-test-output :as humane-test-output]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(humane-test-output/activate!)

;;;; test [[db-types]] and tooling

;;; The DB types stuff below is used to run tests against multiple types of DBs. It's similar to how the `DRIVERS` env
;;; var works in Metabase tests.
;;;
;;; [[db-types]] -- the set of all possible types that we can run tests against. Comes from `TEST_DBS`. In the REPL,
;;; change this with [[set-db-types!]].
;;;
;;; The macro [[do-db-types]] is used to write tests that run once for each enabled test DB type, optionally
;;; restricted to some subset of the enabled types using a predicate function

(defn- db-types-from-env
  ([]
   (db-types-from-env (or (System/getenv "TEST_DBS")
                          (System/getProperty "test.dbs"))))
  ([s]
   (when (string? s)
     (not-empty (set (for [s (str/split (str/lower-case (str/trim s)) #"\s*,\s*")
                           :when (seq s)]
                       (keyword s)))))))

(deftest db-types-from-env-test
  (are [s expected] (is (= expected
                           (db-types-from-env s)))
    nil           nil
    ""            nil
    " "           nil
    "postgres"    #{:postgres}
    "postgres,h2" #{:postgres :h2}))

(defonce ^:private db-types*
  (atom (or (db-types-from-env)
            #{:postgres :h2})))

(defn- db-types
  "The enabled test DB types that we should run tests against inside of [[for-all-db-types]] forms."
  []
  (set @db-types*))

(def ^:dynamic ^:private *current-db-type*
  nil)

(defn current-db-type []
  (or *current-db-type*
      ;; default to `:postgres` if it's enabled.
      (when (contains? (db-types) :postgres)
        :postgres)
      (first (db-types))))

(println "Running tests against DB types:" (pr-str (db-types)))

(defn do-db-types-fixture
  "[[clojure.test]] fixture for running tests in a namespace against various db types using [[do-db-types]].

    ;; run tests in this namespace against all the enabled test [[db-types]]
    (use-fixtures :each (do-db-types-fixture))"
  [thunk]
  (doseq [db-type (db-types)]
    (binding [*current-db-type* db-type]
      (testing (str db-type \newline)
        (thunk)))))

;;;; URLs for test DBs.

(defmulti ^:private default-test-db-url
  {:arglists '([db-type])}
  keyword)

(defmethod default-test-db-url :postgres
  [_db-type]
  "jdbc:postgresql://localhost:5432/toucan2?user=cam&password=cam")

(defmethod default-test-db-url :h2
  [_db-type]
  "jdbc:h2:mem:toucan2;DB_CLOSE_DELAY=-1")

(defn- test-db-url [db-type]
  (let [env-var (format "JDBC_URL_%s" (str/upper-case (name db-type)))]
    (or (System/getenv env-var)
        (default-test-db-url db-type))))

;;;; creating test tables, and the default test models.

(m/defmulti create-table-sql-file
  {:arglists '([db-type model-or-table-name])}
  (fn [db-type model-or-table-name]
    [(keyword db-type) (keyword model-or-table-name)]))

(m/defmethod create-table-sql-file [:postgres :people] [_db-type _table] "toucan2/test/people.postgres.sql")
(m/defmethod create-table-sql-file [:h2 :people]       [_db-type _table] "toucan2/test/people.h2.sql")
(m/defmethod create-table-sql-file [:postgres :venues] [_db-type _table] "toucan2/test/venues.postgres.sql")
(m/defmethod create-table-sql-file [:h2 :venues]       [_db-type _table] "toucan2/test/venues.h2.sql")
(m/defmethod create-table-sql-file [:default :birds]   [_db-type _table] "toucan2/test/birds.sql")

(derive ::people ::models)

(m/defmethod model/table-name ::people
  [_model]
  "people")

(derive ::venues ::models)

(m/defmethod model/table-name ::venues
  [_model]
  "venues")

(derive ::birds ::models)

(m/defmethod model/table-name ::birds
  [_model]
  "birds")

(defn- create-table-statements [db-type table-name]
  (let [filename (create-table-sql-file db-type table-name)
        file     (some-> (io/resource filename) io/file)]
    (assert (some-> file .exists) (format "File does not exist: %s" (pr-str filename)))
    (for [stmt (str/split (str/trim (slurp file)) #";")]
      (str/trim stmt))))

(defn create-table!
  "Create the table named `table-name` for the [[current-db-type]] using the SQL from [[create-table-sql-file]]."
  ([table-name]
   (create-table! (current-db-type) table-name))

  ([db-type table-name]
   (binding [*current-db-type* db-type]
     (conn/with-connection [conn ::test/db]
       (create-table! db-type conn table-name))))

  ([db-type ^java.sql.Connection conn table-name]
   (try
     (doseq [^String sql (create-table-statements db-type table-name)]
       (with-open [stmt (.createStatement conn)]
         (try
           (.execute stmt sql)
           (catch Throwable e
             (throw (ex-info (format "Error executing SQL: %s" (ex-message e))
                             {:sql sql}
                             e))))))
     (catch Throwable e
       (throw (ex-info (format "Error creating table %s: %s" table-name (ex-message e))
                       {:table table-name}
                       e))))))

;;;; test DB init and test connectables.

(def ^:private initialized-test-dbs (atom #{}))

(defn- set-up-test-db! [db-type]
  (when-not (contains? @initialized-test-dbs db-type)
    (println "Set up" db-type "test DB")
    (with-open [conn (java.sql.DriverManager/getConnection (test-db-url db-type))]
      (doseq [table-name [:people
                          :venues
                          :birds]]
        (create-table! db-type conn table-name)))
    (swap! initialized-test-dbs conj db-type)))

(m/defmethod conn/do-with-connection ::db
  [_connectable f]
  (let [db-type (current-db-type)]
    (set-up-test-db! db-type)
    (conn/do-with-connection (test-db-url db-type) f)))

(defn do-with-discarded-table-changes [db-type table-name thunk]
  (try
    (thunk)
    (finally
      (with-open [conn (java.sql.DriverManager/getConnection (test-db-url db-type))]
        (create-table! db-type conn table-name)))))

(defmacro with-discarded-table-changes
  {:style/indent 1}
  [table-name & body]
  `(do-with-discarded-table-changes (current-db-type) ~table-name (^:once fn* [] ~@body)))

(s/fdef with-discarded-table-changes
  :args (s/cat :table-name (some-fn symbol? keyword?)
               :body       (s/+ any?))
  :ret  any?)

(m/defmethod model/default-connectable ::models
  [_model]
  ::db)

;;;; conveniences for REPL-based usage. These are not used in tests.

(defn set-db-types!
  "Change the DB types to run tests against for the current REPL session."
  [& db-types]
  {:pre [(every? keyword? db-types) (seq db-types)]}
  (reset! db-types* (set db-types)))

(derive ::convenience-connectable ::db)
(derive :repl/h2                  ::convenience-connectable)
(derive :repl/postgres            ::convenience-connectable)

(m/defmethod conn/do-with-connection ::convenience-connectable
  [connectable f]
  (let [db-type (keyword (name connectable))]
    (binding [*current-db-type* db-type]
      (conn/do-with-connection ::db f))))

;;;; misc print methods

(defmethod print-dup java.time.LocalDateTime
  [t writer]
  (print-dup (list 'java.time.LocalDateTime/parse (str t))
             writer))

(defmethod print-method java.time.LocalDateTime
  [t writer]
  (print-method (list 'java.time.LocalDateTime/parse (str t))
                writer))
