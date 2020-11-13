(ns bluejdbc.statement-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.options :as options]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(deftest proxy-result-set-test
  (testing "Calling .executeQuery on a ProxyPreparedStatement should return a ProxyResultSet"
    (let [options {:x :y}]
      (with-open [conn (jdbc/connect! (test/jdbc-url) options)
                  stmt (jdbc/prepare! conn "SELECT 1 AS one;")
                  rs   (.executeQuery stmt)]
        (is (instance? bluejdbc.result_set.ProxyResultSet rs))

        (testing "with the same options"
          (is (= {:x :y}
                 (select-keys (options/options rs) [:x]))))

        (testing "the ProxyResultSet should be able to return its parent ProxyPreparedStatement"
          (is (identical? stmt (.getStatement rs)))

          (testing "and its parent ProxyConnection"
            (is (identical? conn (.. rs getStatement getConnection)))))))))

(deftest reducible-test
  (testing "ProxyPreparedStatements should be reducible"
    (with-open [conn (jdbc/connect! (test/jdbc-url))
                stmt (jdbc/prepare! conn "SELECT 1 AS one;")]
      (is (= (test/results [{:one 1}])
             (reduce conj stmt))))))

(deftest honeysql-options-test
  (testing "Make sure HoneySQL options are applied"
    (let [options {:honeysql/quoting             (case (test/db-type)
                                                   (:mysql :mariadb) :mysql
                                                   :ansi)
                   :honeysql/allow-dashed-names? true}]
      (with-open [conn (jdbc/connect! (test/jdbc-url) options)
                  stmt (jdbc/prepare! conn
                                      {:select [[(t/offset-date-time "2020-04-15T07:04:02.465161Z") :my-date]]})]
        (is (= [{:my-date (t/offset-date-time "2020-04-15T07:04:02.465161Z")}]
               (reduce conj stmt)))))))
