(ns bluejdbc.query-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(defn- wacky-row-xform [_]
  (fn [rf]
    (fn
      ([] (rf))
      ([acc] (rf acc))
      ([acc row] (rf acc (cons :row row))))))

(deftest query-test
  (let [t   (t/offset-date-time "2020-04-15T07:04:02.465161Z")
        sql "SELECT ? AS t;"]
    (jdbc/with-connection [conn (test/jdbc-url)]
      (testing "(query"
        (testing "conn query)"
          (is (= [{:t t}]
                 (jdbc/query conn [sql t]))))

        (testing "stmt)"
          (is (= [{:t t}]
                 (jdbc/with-prepared-statement [stmt conn [sql t]]
                   (jdbc/query stmt)))))

        (doseq [[options expected] {nil
                                    [{:t t}]

                                    {:results/xform nil}
                                    [[t]]

                                    {:results/xform wacky-row-xform}
                                    [[:row t]]}]
          (testing (format "conn query %s)" (pr-str options))
            (is (= expected
                   (jdbc/query conn [sql t] options))))

          (jdbc/with-prepared-statement [stmt conn [sql t]]
            (testing (format "stmt %s)" (pr-str options))
              (is (= expected
                     (jdbc/query stmt options))))

            (testing (format "conn stmt %s)" (pr-str options))
              (is (= expected
                     (jdbc/query conn stmt options))))))))))

(deftest query-one-test
  (is (= {:abc "abc"}
         (jdbc/query-one (test/jdbc-url) "SELECT 'abc' AS abc;")))

  (testing "With a custom results xform"
    (is (= ["abc"]
           (jdbc/query-one (test/jdbc-url) "SELECT 'abc' AS abc;" {:results/xform nil})))))

(deftest execute-test
  (testing "execute!"
    (jdbc/with-connection [conn (test/jdbc-url)]
      (try
        (is (= 0
               (jdbc/execute! conn "CREATE TABLE execute_test_table (id INTEGER NOT NULL);")))
        (is (= []
               (jdbc/query conn "SELECT * FROM execute_test_table;")))
        (finally
          (jdbc/execute! conn "DROP TABLE IF EXISTS execute_test_table;"))))))

(deftest insert-test
  (testing "insert!"
    (doseq [[description f] {"with row maps"
                             (fn [conn]
                               (jdbc/insert! conn :execute_test_table [{:id 1, :name "Cam"}
                                                                       {:id 2, :name "Sam"}]))

                             "with row vectors"
                             (fn [conn]
                               (jdbc/insert! conn :execute_test_table [:id :name] [[1 "Cam"] [2 "Sam"]]))}]
      (testing description
        (jdbc/with-connection [conn (test/jdbc-url)]
          (try
            (is (= 0
                   (jdbc/execute! conn "CREATE TABLE execute_test_table (id INTEGER NOT NULL, name TEXT NOT NULL);")))
            (is (= 2
                   (f conn)))
            (is (= [{:id 1, :name "Cam"}
                    {:id 2, :name "Sam"}]
                   (jdbc/query conn {:select   [:*]
                                     :from     [:execute_test_table]
                                     :order-by [[:id :asc]]})))
            (finally
              (jdbc/execute! conn "DROP TABLE IF EXISTS execute_test_table;"))))))))
