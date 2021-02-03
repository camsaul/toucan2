(ns bluejdbc.test.load
  "Code for creating DBs and loading up test data to facilitate testing."
  (:require [bluejdbc.connection :as connection]
            [bluejdbc.core :as jdbc]
            [bluejdbc.util :as u]
            [clojure.string :as str]
            [clojure.tools.reader.edn :as edn]
            [java-time :as t]
            [methodical.core :as m]))

(m/defmulti create-database-with-test-data!
  {:arglists '([conn data])}
  (u/dispatch-on-first-arg-with connection/db-type))

(m/defmulti quote-identifier
  {:arglists '([db-type s])}
  u/dispatch-on-first-arg)

(m/defmethod quote-identifier :default
  [_ s]
  (when s
    ;; TODO -- not sure if this is the right way to escape double quotes
    (str \" (str/replace (u/qualified-name s) #"[\"]" "\\\"") \")))

(defn comma-separated [values]
  (when (seq values)
    (str/join ", " values)))

#_(defn parens-list [values]
  (when (seq values)
    (str \( (comma-separated values) \))))

(m/defmulti class->equivalent-column-type
  {:arglists '([db-type column-class])}
  (fn [db-type column-class]
    (let [klass (if (symbol? column-class)
                  (resolve column-class)
                  column-class)]
      [(u/keyword-or-class db-type) klass])))

(m/defmethod class->equivalent-column-type [:default String]                   [_ _] "TEXT")
(m/defmethod class->equivalent-column-type [:default Integer]                  [_ _] "INTEGER")
(m/defmethod class->equivalent-column-type [:default Long]                     [_ _] "INTEGER")
(m/defmethod class->equivalent-column-type [:default Boolean]                  [_ _] "BOOLEAN")
(m/defmethod class->equivalent-column-type [:default java.time.OffsetDateTime] [_ _] "TIMESTAMP WITH TIME ZONE")
(m/defmethod class->equivalent-column-type [:default java.time.OffsetDateTime] [_ _] "TIMESTAMP WITH TIME ZONE")

(m/defmethod class->equivalent-column-type
  [:postgresql :bluejdbc.test/autoincrement]
  [_ _]
  "SERIAL")

(m/defmulti create-table-ddl
  {:arglists '([db-type table])}
  u/dispatch-on-first-arg)

(m/defmulti drop-table-if-exists-ddl
  {:arglists '([db-type table])}
  u/dispatch-on-first-arg)

(m/defmethod drop-table-if-exists-ddl :default
  [db-type {table-name :name}]
  (format "DROP TABLE IF EXISTS %s;" (quote-identifier db-type table-name)))

(m/defmulti drop-table-if-exists!
  {:arglists '([conn table])}
  (u/dispatch-on-first-arg-with connection/db-type))

(m/defmethod drop-table-if-exists! :default
  [conn {table-name :name, :as table}]
  (let [ddl (drop-table-if-exists-ddl (connection/db-type conn) table)]
    (try
      (jdbc/execute! conn ddl)
      (catch Throwable e
        (throw (ex-info (format "Error executing DROP TABLE DDL statement for %s" (pr-str table-name))
                        {:ddl ddl}
                        e))))))

(m/defmethod drop-table-if-exists! :around :default
  [conn {table-name :name, :as table}]
  (try
    (next-method conn table)
    (catch Throwable e
      (throw (ex-info (format "Error dropping table %s" (pr-str table-name))
                      {}
                      e)))))

(m/defmethod create-table-ddl :default
  [db-type {table-name :name, :keys [columns primary-keys]}]
  (format "CREATE TABLE %s (%s)"
          (quote-identifier db-type table-name)
          (comma-separated
           (concat
            (for [{column-name :name, column-class :class, :keys [not-null?]} columns]
              (str (format "%s %s" (quote-identifier db-type column-name)
                           (class->equivalent-column-type db-type column-class))
                   (when not-null?
                     (str " NOT NULL"))))
            (when (seq primary-keys)
              [(format "PRIMARY KEY (%s)"
                       (comma-separated
                        (for [k primary-keys]
                          (quote-identifier db-type k))))])))))

(m/defmulti insert-rows!
  {:arglists '([conn table])}
  (u/dispatch-on-first-arg-with connection/db-type))

(m/defmethod insert-rows! :default
  [conn {table-name :name, :keys [columns rows]}]
  (when (seq rows)
    (jdbc/insert! conn table-name (map :name columns) rows)))

(m/defmethod insert-rows! :around :default
  [conn {table-name :name, :as table}]
  (try
    (next-method conn table)
    (catch Throwable e
      (throw (ex-info (format "Error inserting rows into table %s" (pr-str table-name))
                      {}
                      e)))))

(m/defmulti create-table-and-insert-rows!
  {:arglists '([conn table])}
  (u/dispatch-on-first-arg-with connection/db-type))

(m/defmethod create-table-and-insert-rows! :default
  [conn {table-name :name, :as table}]
  (drop-table-if-exists! conn table)
  (let [ddl (create-table-ddl (connection/db-type conn) table)]
    (try
      (jdbc/execute! conn ddl)
      (catch Throwable e
        (throw (ex-info (format "Error executing CREATE TABLE DDL statement for %s" (pr-str table-name))
                        {:ddl ddl}
                        e)))))
  (insert-rows! conn table))

(m/defmethod create-table-and-insert-rows! :around :default
  [conn {table-name :name, :as table}]
  (try
    (next-method conn table)
    (catch Throwable e
      (throw (ex-info (format "Error creating table %s" (pr-str table-name))
                      {:table (update table :rows (partial take 10))}
                      e)))))

(m/defmethod create-database-with-test-data! :default
  [conn tables]
  (doseq [table tables]
    (create-table-and-insert-rows! conn table)))

(m/defmulti destroy-all-tables!
  {:arglists '([conn data-source])}
  (u/dispatch-on-first-arg-with connection/db-type))

(m/defmethod destroy-all-tables! :default
  [conn tables]
  (doseq [table tables]
    (drop-table-if-exists! conn table)))

(m/defmulti data
  {:arglists '([data-source])}
  u/keyword-or-class)

(m/defmethod data String
  [^String filename]
  (with-open [r (java.io.PushbackReader. (java.io.FileReader. "test/bluejdbc/test/people.edn"))]
    (edn/read
     {:eof nil, :readers {'offset-date-time t/offset-date-time}}
     r)))

(m/defmethod data clojure.lang.Sequential
  [coll]
  coll)

(m/defmethod data :people
  [_]
  (data "test/bluejdbc/test/people.edn"))
