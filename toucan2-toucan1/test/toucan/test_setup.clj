(ns toucan.test-setup
  (:require [methodical.core :as m]
            [toucan2.test :as test]))

(derive ::test-connectable :toucan1/connectable)
(derive ::test-connectable :test/postgres)

;;; Basic Setup

;; always reload Toucan test data when calling load-test-data-if-needed
(m/defmethod test/has-test-data?* [::test-connectable :default]
  [_ _]
  #_[connectable table-name]
  #_(str "t1_" (csk/->snake_case (name table-name)))
  false)

(m/defmethod test/load-test-data-if-needed!* [:default :toucan1/users]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS t1_users (
      id SERIAL PRIMARY KEY,
      \"first-name\" VARCHAR(256) NOT NULL,
      \"last-name\" VARCHAR(256) NOT NULL
    );"
   "TRUNCATE TABLE t1_users RESTART IDENTITY CASCADE;"
   "INSERT INTO t1_users (\"first-name\", \"last-name\")
    VALUES
    ('Cam', 'Saul'),
    ('Rasta', 'Toucan'),
    ('Lucky', 'Bird');"])

(m/defmethod test/load-test-data-if-needed!* [:default :toucan1/venues]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS t1_venues (
      id SERIAL PRIMARY KEY,
      name VARCHAR(256) UNIQUE NOT NULL,
      category VARCHAR(256) NOT NULL,
      \"created-at\" TIMESTAMP NOT NULL,
      \"updated-at\" TIMESTAMP NOT NULL
    );"
   "TRUNCATE TABLE t1_venues RESTART IDENTITY CASCADE;"
   "INSERT INTO t1_venues (name, category, \"created-at\", \"updated-at\")
    VALUES
    ('Tempest', 'bar', '2017-01-01 00:00:00', '2017-01-01 00:00:00'),
    ('Ho''s Tavern', 'bar', '2017-01-01 00:00:00', '2017-01-01 00:00:00'),
    ('BevMo', 'store', '2017-01-01 00:00:00', '2017-01-01 00:00:00');"])

(m/defmethod test/load-test-data-if-needed!* [:default :toucan1/categories]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS t1_categories (
      id SERIAL PRIMARY KEY,
      name VARCHAR(256) UNIQUE NOT NULL,
      \"parent-category-id\" INTEGER
    );"
   "TRUNCATE TABLE t1_categories RESTART IDENTITY CASCADE;"
   "INSERT INTO t1_categories (name, \"parent-category-id\")
    VALUES
    ('bar', NULL),
    ('dive-bar', 1),
    ('resturaunt', NULL),
    ('mexican-resturaunt', 3);"])

(m/defmethod test/load-test-data-if-needed!* [:default :toucan1/addresses]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS t1_address (
      id SERIAL PRIMARY KEY,
      street_name text NOT NULL
    );"
   "TRUNCATE TABLE t1_address RESTART IDENTITY CASCADE;"
   "INSERT INTO t1_address (street_name) VALUES ('1 Toucan Drive');"])

(m/defmethod test/load-test-data-if-needed!* [:default :toucan1/phone-numbers]
  [_ _]
  ["CREATE TABLE IF NOT EXISTS t1_phone_numbers (
      number TEXT PRIMARY KEY,
      country_code VARCHAR(3) NOT NULL
    );"
   "TRUNCATE TABLE t1_phone_numbers;"])

(defn reset-db!
  "Reset the DB to its initial state, creating tables if needed and inserting the initial test data."
  []
  (doseq [table [:toucan1/users
                 :toucan1/venues
                 :toucan1/categories
                 :toucan1/addresses
                 :toucan1/phone-numbers]]
    (test/load-test-data-if-needed!* ::test-connectable table)))

(defn do-with-reset-db
  "Intended as a test fixture."
  [thunk]
  (reset-db!)
  (thunk))

(defmacro with-clean-db
  "Run test `body` and reset the database to its initial state afterwards."
  {:style/indent 0}
  [& body]
  `(try ~@body
        (finally (reset-db!))))

(defn do-with-default-connection
  [thunk]
  (try
    (derive :toucan2/default ::test-connectable)
    #_(derive :test/postgres :toucan1/connectable)
    (test/do-with-default-connection thunk)
    (finally
      (underive :toucan2/default ::test-connectable)
      #_(underive :test/postgres :toucan1/connectable))))
