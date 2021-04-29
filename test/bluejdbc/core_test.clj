(ns bluejdbc.core-test
  (:require [bluejdbc.core :as blue]
            [bluejdbc.test :as test]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [java-time :as t]))

(use-fixtures :once test/do-with-test-data)
(use-fixtures :each test/do-with-default-connection)

;; Examples

;; define a connection.
(blue/defmethod blue/connection* ::pg-connection
  [_ options]
  (blue/connection* "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam" options))

;; Use the bluejdbc Postgres integrations.
(require 'bluejdbc.integrations.postgresql)
(derive ::pg-connection :bluejdbc.integrations/postgres)

(deftest basic-query-example
  ;; run a query.
  (is (= [(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
          (blue/instance :people {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
          (blue/instance :people {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
          (blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
         (blue/select [::pg-connection :people]))))


;; Define the primary connection.
#_(blue/defmethod blue/connection* :bluejdbc/default
    [_ _]
    "jdbc:postgresql://localhost:5432/bluejdbc?user=cam&password=cam")

;; Some basic queries.
(deftest basic-queries-test
  (testing "fetch all :people"
    (is (= [(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
            (blue/instance :people {:id 2, :name "Sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
            (blue/instance :people {:id 3, :name "Pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
            (blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
           (blue/select :people))))

  (testing "Fetch :people with PK = 1"
    (is (= [(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
           (blue/select :people 1))))

  (testing "Fetch :people with :name = Cam"
    (is (= [(blue/instance :people {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
           (blue/select :people :name "Cam"))))

  (testing "Fetch :people with arbitrary HoneySQL"
    (is (= [(blue/instance :people {:id 4, :name "Tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
           (blue/select :people {:order-by [[:id :desc]], :limit 1})))))

;; "Pre-select"

;; create a new version of the "people" table that returns ID and name by default.
(blue/defmethod blue/select* :before [::pg-connection :people/default-fields clojure.lang.IPersistentMap]
  [_ _ query _]
  (merge {:select [:id :name]} query))

(deftest pre-select-test
  (is (= [(blue/instance :people/default-fields {:id 1, :name "Cam"})
          (blue/instance :people/default-fields {:id 2, :name "Sam"})
          (blue/instance :people/default-fields {:id 3, :name "Pam"})
          (blue/instance :people/default-fields {:id 4, :name "Tam"})]
         (blue/select [::pg-connection :people/default-fields]))))

;; create a new version of "people" that converts :name to lowercase.
(blue/defmethod blue/select* :after [::pg-connection :people/lower-case-names :default]
  [_ _ results _]
  (for [result results]
    (update result :name str/lower-case)))

(deftest post-select-test
  (is (= [(blue/instance :people/lower-case-names {:id 1, :name "cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})
          (blue/instance :people/lower-case-names {:id 2, :name "sam", :created_at (t/offset-date-time "2019-01-11T23:56:00Z")})
          (blue/instance :people/lower-case-names {:id 3, :name "pam", :created_at (t/offset-date-time "2020-01-01T21:56:00Z")})
          (blue/instance :people/lower-case-names {:id 4, :name "tam", :created_at (t/offset-date-time "2020-05-25T19:56:00Z")})]
         (blue/select [::pg-connection :people/lower-case-names]))))

;; Combine multiple actions together!

(derive :people/default :people/default-fields)
(derive :people/default :people/lower-case-names)

(deftest pre-and-post-test
  (is (= [(blue/instance :people/default {:id 1, :name "cam"})
          (blue/instance :people/default {:id 2, :name "sam"})
          (blue/instance :people/default {:id 3, :name "pam"})
          (blue/instance :people/default {:id 4, :name "tam"})]
         (blue/select [::pg-connection :people/default]))))

;; Use a non-integer or non-`:id` primary key.
(blue/defmethod blue/primary-key* [::pg-connection :people/name-pk]
  [_ _]
  :name)

(deftest alternate-primary-keys-test
  (is (= [(blue/instance :people/name-pk {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
         (blue/select [::pg-connection :people/name-pk] "Cam"))))

;; Use a composite PK (!)
(blue/defmethod blue/primary-key* [::pg-connection :people/composite-pk]
  [_ _]
  [:id :name])

(deftest composite-pk-test
  (is (= [(blue/instance :people/composite-pk {:id 1, :name "Cam", :created_at (t/offset-date-time "2020-04-21T23:56:00Z")})]
         (blue/select [::pg-connection :people/composite-pk] [1 "Cam"]))))

;; TODO -- maps keep track of their changes!
