(ns bluejdbc.result-set-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(deftest reducible-test
  (testing "Make sure ResultSets are reducible"
    (with-open [conn (jdbc/connect! (test/jdbc-url))
                stmt (jdbc/prepare! conn "SELECT 1 AS one;")
                rs   (jdbc/results stmt)]
      (is (= [{:one 1}]
             (reduce conj rs)))

      (testing "using a custom row transform"
        (with-open [rs2 (jdbc/results stmt {:results/xform nil})]
          (is (= [[1]]
                 (reduce conj rs2))))))))

(deftest seq-test
  (testing "Should be able to treat a ProxyResultSet as a sequence"
    (with-open [conn (jdbc/connect! (test/jdbc-url))
                stmt (jdbc/prepare! conn "SELECT 1 AS one;")
                rs   (jdbc/results stmt)]
      (is (= [{:one 1}]
             (vec rs)))

      (testing "using a custom row transform"
        (with-open [rs2 (jdbc/results stmt {:results/xform nil})]
          (is (= [[1]]
                 (vec rs2))))))))

(deftest alternative-row-transforms-test
  (testing "Should be able to return rows as"
    (jdbc/with-connection [conn (test/jdbc-url)]
      (test/with-test-data conn
        (with-open [stmt (jdbc/prepare! conn "SELECT * FROM people ORDER BY id ASC;")]
          (doseq [[description {:keys [xform expected]}]
                  {"none (vectors)"
                   {:xform    nil
                    :expected [[1 "Cam" (t/local-date-time "2020-04-21T16:56")]
                               [2 "Sam" (t/local-date-time "2019-01-11T15:56")]]}

                   "namespaced maps"
                   {:xform    rs/namespaced-maps-xform
                    :expected [{:people/id         1
                                :people/name       "Cam"
                                :people/created_at (t/local-date-time "2020-04-21T16:56")}
                               {:people/id         2
                                :people/name       "Sam"
                                :people/created_at (t/local-date-time "2019-01-11T15:56" )}]}}]
            (testing description
              (is (= expected
                     (transduce (take 2) conj [] (stmt/results stmt {:results/xform xform}))))

              (testing "with-prepared-statement"
                (is (= expected
                       (stmt/with-prepared-statement [stmt conn stmt {:results/xform xform}]
                         (transduce (take 2) conj [] (stmt/results stmt)))))))))))))
