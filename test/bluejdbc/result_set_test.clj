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
      (is (= (test/results [{:one 1}])
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
      (is (= (test/results [{:one 1}])
             (vec rs)))

      (testing "using a custom row transform"
        (with-open [rs2 (jdbc/results stmt {:results/xform nil})]
          (is (= [[1]]
                 (vec rs2))))))))

(deftest alternative-row-transforms-test
  (jdbc/with-connection [conn (test/jdbc-url)]
    (testing "Should be able to return rows as"
      (test/with-test-data conn
        (with-open [stmt (jdbc/prepare! conn "SELECT * FROM people ORDER BY id ASC;")]
          (doseq [[description {:keys [xform expected]}]
                  {"none (vectors)"
                   {:xform    nil
                    :expected [[1 "Cam" (t/local-date-time "2020-04-21T23:56")]
                               [2 "Sam" (t/local-date-time "2019-01-11T23:56")]]}

                   "namespaced maps"
                   {:xform    (rs/maps :namespaced)
                    :expected [{:people/id         1
                                :people/name       "Cam"
                                :people/created_at (t/local-date-time "2020-04-21T23:56")}
                               {:people/id         2
                                :people/name       "Sam"
                                :people/created_at (t/local-date-time "2019-01-11T23:56" )}]}

                   "namespaced lisp-case maps"
                   {:xform    (rs/maps :namespaced :lisp-case)
                    :expected [{:people/id         1
                                :people/name       "Cam"
                                :people/created-at (t/local-date-time "2020-04-21T23:56")}
                               {:people/id         2
                                :people/name       "Sam"
                                :people/created-at (t/local-date-time "2019-01-11T23:56" )}]}

                   "results-xform should be comp-able"
                   {:xform (fn [rs]
                             (comp ((rs/maps :namespaced :lisp-case) rs)
                                   (map (test/identifier :people/id))))
                    :expected [1 2]}}]
            (testing description
              (is (= (test/results expected)
                     (transduce (take 2) conj [] (stmt/results stmt {:results/xform xform}))))

              (testing "with-prepared-statement"
                (is (= (test/results expected)
                       (stmt/with-prepared-statement [stmt conn stmt {:results/xform xform}]
                         (transduce (take 2) conj [] (stmt/results stmt)))))))))))))

(deftest transform-column-names-test
  (jdbc/with-connection [conn (test/jdbc-url)]
    (letfn [(column-names [options]
              (set (keys (jdbc/query-one conn
                                         {:select [[1 "AbC_dEF"] [2 "ghi-JKL"]]}
                                         (merge {:honeysql/quoting (case (test/db-type)
                                                                     (:mysql :mariadb) :mysql
                                                                     :ansi)}
                                                options)))))]
      (testing "sanity check"
        (is (= #{:AbC_dEF :ghi-JKL}
               (column-names nil))))

      (testing "Should be able to return"
        (testing "lower-cased identifiers"
          (is (= #{:abc_def :ghi-jkl}
                 (column-names {:results/xform (jdbc/maps :lower-case)}))))

        (testing "upper-cased identifiers"
          (is (= #{:ABC_DEF :GHI-JKL}
                 (column-names {:results/xform (jdbc/maps :upper-case)}))))

        (testing "lisp-cased identifiers"
          (is (= #{:AbC-dEF :ghi-JKL}
                 (column-names {:results/xform (jdbc/maps :lisp-case)}))))

        (testing "snake-cased identifiers"
          (is (= #{:AbC_dEF :ghi_JKL}
                 (column-names {:results/xform (jdbc/maps :snake-case)}))))

        (testing "lower lisp-cased identifiers"
          (is (= #{:abc-def :ghi-jkl}
                 (column-names {:results/xform (jdbc/maps :lower-case :lisp-case)}))))

        (testing "upper lisp-cased identifiers"
          (is (= #{:ABC-DEF :GHI-JKL}
                 (column-names {:results/xform (jdbc/maps :upper-case :lisp-case)}))))))))

(deftest time-columns-test
  (is (= [(t/local-time "16:57:09")]
         (jdbc/query-one (test/jdbc-url) ["SELECT CAST(? AS time)" (t/local-time "16:57:09")] {:results/xform nil}))))

(deftest date-columns-test
  (testing "Make sure fetching DATE columns works correctly"
    (is (= [(t/local-date "2020-04-23")]
           (jdbc/query-one (test/jdbc-url) ["SELECT CAST(? AS date)" (t/local-date "2020-04-23")] {:results/xform nil})))))

(deftest datetime-columns-test
  (testing "Make sure fetching DATETIME columns works correctly"
    ;; Postgres doesn't have a DATETIME type
    (test/exclude #{:postgresql}
      (is (= [(t/local-date-time "2020-04-23T16:57:09")]
             (jdbc/query-one (test/jdbc-url)
                             ["SELECT CAST(? AS datetime)" (t/local-date-time "2020-04-23T16:57:09")]
                             {:results/xform nil}))))))

(deftest timestamp-columns-test
  (testing "Make sure fetching TIMESTAMP (without time zone) columns works correctly"
    (is (= [(t/local-date-time "2020-04-23T16:57:09")]
           (jdbc/query-one (test/jdbc-url)
                           [(case (test/db-type)
                              :mysql "SELECT timestamp(?)"
                              "SELECT CAST(? AS timestamp)")
                            (t/local-date-time "2020-04-23T16:57:09")]
                           {:results/xform nil})))))

;; TODO -- timestamp with time zone

;; TODO -- time with time zone (for DBs that support it)
