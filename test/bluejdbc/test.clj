(ns bluejdbc.test
  "Test utils."
  (:require [clojure.string :as str]
            [environ.core :as env]))

(defn jdbc-url
  "JDBC URL to run tests against."
  ^String []
  (let [url (env/env :jdbc-url)]
    (assert (not (str/blank? url)))
    url))

(defn db-type
  "Type of database we're tetsing against, e.g. `:postgresql`."
  []
  (keyword (second (re-find #"^jdbc:([^:]+):" (jdbc-url)))))

(def ^:dynamic *db-type* nil)

(defn do-only [dbs thunk]
  (let [db-type (db-type)]
    (doseq [db (if (keyword? dbs)
                 [dbs]
                 (set dbs))]
      (when (= db db-type)
        (binding [*db-type* db]
          (thunk))))))

(defmacro only
  "Only run `body` against DBs if we are currently testing against them."
  {:style/indent 1}
  [dbs & body]
  `(do-only ~dbs (fn [] ~@body)))
