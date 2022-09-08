(ns toucan2.test
  (:require
   [clojure.java.io :as io]
   [clojure.spec.alpha :as s]
   [clojure.string :as str]
   [clojure.test :as t]
   [environ.core :as env]
   [honey.sql :as hsql]
   [humane-are.core :as humane-are]
   [methodical.core :as m]
   [pjstadig.humane-test-output :as humane-test-output]
   [puget.printer :as puget]
   [toucan2.connection :as conn]
   [toucan2.log :as log]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.model :as model]))

(set! *warn-on-reflection* true)

(humane-are/install!)
(humane-test-output/activate!)

;;; make sure ALL of our tests are either marked `^:parallel` or `^:synchronized`, that way we can run things quick in
;;; the test runner.

(defn- has-parallel-or-synchronized-metadata? [symb]
  ((some-fn :parallel :synchronized) (meta symb)))

(s/fdef t/deftest
  :args (s/cat :test-name (every-pred symbol? has-parallel-or-synchronized-metadata?)
               :body      (s/+ any?))
  :ret  any?)

;;;; test [[db-types]] and tooling

;;; The DB types stuff below is used to run tests against multiple types of DBs. It's similar to how the `DRIVERS` env
;;; var works in Metabase tests.
;;;
;;; [[db-types]] -- the set of all possible types that we can run tests against. Comes from `TEST_DBS`. In the REPL,
;;; change this with [[set-db-types!]].

(defn- db-types-from-env
  ([]
   (db-types-from-env (env/env :test-dbs)))
  ([s]
   (when (string? s)
     (not-empty (set (for [s (str/split (str/lower-case (str/trim s)) #"\s*,\s*")
                           :when (seq s)]
                       (keyword s)))))))

(t/deftest ^:parallel db-types-from-env-test
  (t/are [s expected] (= expected
                         (db-types-from-env s))
    nil           nil
    ""            nil
    " "           nil
    "postgres"    #{:postgres}
    "postgres,h2" #{:postgres :h2}))

(defonce ^:private db-types*
  (atom (or (db-types-from-env)
            #{:h2})))

(defn- db-types
  "The enabled test DB types that we should run tests against."
  []
  (set @db-types*))

(def ^:dynamic ^:private *current-db-type*
  nil)

(defn current-db-type []
  (or *current-db-type*
      ;; default to `:h2` if it's enabled.
      (when (contains? (db-types) :h2)
        :h2)
      (first (db-types))))

(println "Running tests against DB types:" (pr-str (db-types)))

;;;; custom version of [[t/test-var]]

(defonce ^:private orig-test-var t/test-var)

;;; This is only used by [[toucan2.test-runner]] but it lives here so we don't have to have ANOTHER function that wraps
;;; [[t/run-test]] AGAIN inside the test runner code
(def ^:dynamic *parallel-test-counter*
  nil)

(defn parallel? [test-var]
  (assert ((some-fn :parallel :synchronized) (meta test-var))
          (format "Tests must have either ^:parallel or ^:synchronized metadata. Bad test: %s" (pr-str test-var)))
  (:parallel (meta test-var)))

(def ^:private ^:dynamic *parallel-test* false)

(defn- wrap-test-var!
  "Swaps out the `:test` metadata for var so testing it will run the test against all the dbs we're testing."
  [varr]
  (when-not (::original-test (meta varr))
    (alter-meta! varr (fn [mta]
                        (assoc mta ::original-test (:test mta)))))
  (let [orig    (get (meta varr) ::original-test)
        wrapped (fn wrapped-test-fn []
                  (binding [*parallel-test* (when (parallel? varr) varr)]
                    (doseq [db-type (db-types)]
                      (binding [*current-db-type* db-type]
                        (t/testing (str db-type \newline)
                          (orig))))))]
    (alter-meta! varr assoc :test wrapped)))

(defn- test-var*
  "Run a single test `test-var`. Wraps/replaces [[t/test-var]]."
  [varr]
  (some-> *parallel-test-counter* (swap! update
                                         (if (parallel? varr)
                                           :parallel
                                           :single-threaded)
                                         (fnil inc 0)))
  (wrap-test-var! varr)
  (orig-test-var varr))
(println "Wrapped" #'t/test-var "as" #'test-var*)

(doto #'t/test-var
  (alter-var-root (constantly test-var*))
  (alter-meta! merge (select-keys (meta #'test-var*) [:ns :name :file :column :line])))

(when-let [cider-test-var (try
                            (requiring-resolve 'cider.nrepl.middleware.test/test-var)
                            (catch Throwable _))]
  (defonce ^:private orig-cider-test-var @cider-test-var)

  (defn- cider-test-var* [varr]
    (wrap-test-var! varr)
    (orig-cider-test-var varr))

  (doto cider-test-var
    (alter-var-root (constantly cider-test-var*))
    (alter-meta! merge (select-keys (meta #'cider-test-var*) [:ns :name :file :column :line])))
  (println "Wrapped" cider-test-var "as" #'cider-test-var*))

;;;; default HoneySQL options

(defn quote-for-current-db [& args]
  (let [f (:quote (hsql/get-dialect :ansi))
        f (case (current-db-type)
            :postgres f
            :h2       (comp str/upper-case f))]
    (apply f args)))

(hsql/register-dialect!
 ::current-db.dialect
 (assoc (hsql/get-dialect :ansi)
        :quote quote-for-current-db))

(swap! map.honeysql/global-options assoc :dialect ::current-db.dialect)

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
  (let [env-var (keyword (format "jdbc-url-%s" (name db-type)))]
    (or (env/env env-var)
        (default-test-db-url db-type))))

;;;; creating test tables, and the default test models.

(m/defmulti create-table-sql-file
  {:arglists '([db-type model-or-table-name])}
  (fn [db-type model-or-table-name]
    [(keyword db-type) (keyword model-or-table-name)]))

(m/defmethod create-table-sql-file [:postgres :people]    [_db-type _table] "toucan2/test/people.postgres.sql")
(m/defmethod create-table-sql-file [:h2 :people]          [_db-type _table] "toucan2/test/people.h2.sql")
(m/defmethod create-table-sql-file [:postgres :venues]    [_db-type _table] "toucan2/test/venues.postgres.sql")
(m/defmethod create-table-sql-file [:h2 :venues]          [_db-type _table] "toucan2/test/venues.h2.sql")
(m/defmethod create-table-sql-file [:default :birds]      [_db-type _table] "toucan2/test/birds.sql")
(m/defmethod create-table-sql-file [:default :categories] [_db-type _table] "toucan2/test/categories.sql")

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

(derive ::categories ::models)

(m/defmethod model/table-name ::categories
  [_model]
  "category")

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
     (conn/with-connection [conn ::db]
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
    (locking initialized-test-dbs
      (when-not (contains? @initialized-test-dbs db-type)
        #_(println "Set up" db-type "test DB")
        (with-open [conn (java.sql.DriverManager/getConnection (test-db-url db-type))]
          (doseq [table-name [:people
                              :venues
                              :birds
                              :categories]]
            (create-table! db-type conn table-name)))
        (swap! initialized-test-dbs conj db-type)))))

(m/defmethod conn/do-with-connection ::db
  [_connectable f]
  (let [db-type (current-db-type)]
    (set-up-test-db! db-type)
    (conn/do-with-connection (test-db-url db-type) f)))

(defn do-with-discarded-table-changes [_db-type table-name thunk]
  (assert (not *parallel-test*) (format "with-discarded-table-changes is not allowed inside parallel tests. Test: %s"
                                        *parallel-test*))
  (try
    (thunk)
    (finally
      (create-table! table-name))))

(defmacro with-discarded-table-changes
  {:style/indent 1}
  [table-name & body]
  `(do-with-discarded-table-changes (current-db-type) ~table-name (^:once fn* [] ~@body)))

(defn discard-table-changes-all-dbs! [table-name]
  (doseq [db-type (db-types)]
    (binding [*current-db-type* db-type]
      (create-table! table-name))))

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
  [& ks]
  {:pre [(every? keyword? ks) (seq ks)]}
  (reset! db-types* (set ks)))

(derive ::convenience-connectable ::db)
(derive :repl/h2                  ::convenience-connectable)
(derive :repl/postgres            ::convenience-connectable)

(m/defmethod conn/do-with-connection ::convenience-connectable
  [connectable f]
  (let [db-type (keyword (name connectable))]
    (binding [*current-db-type* db-type]
      (conn/do-with-connection ::db f))))

;;;; misc print methods

(defmethod print-method java.time.LocalDateTime
  [t writer]
  (print-method (list 'java.time.LocalDateTime/parse (str t))
                writer))

(defmethod print-method java.time.OffsetDateTime
  [t writer]
  (print-method (list 'java.time.OffsetDateTime/parse (str t))
                writer))

(defmethod log/print-handler java.time.LocalDateTime
  [_klass]
  (fn [printer t]
    (puget/format-doc printer (list 'java.time.LocalDateTime/parse (str t)))))

(defmethod log/print-handler java.time.OffsetDateTime
  [_klass]
  (fn [printer t]
    (puget/format-doc printer (list 'java.time.OffsetDateTime/parse (str t)))))
