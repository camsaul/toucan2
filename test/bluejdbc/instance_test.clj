(ns bluejdbc.instance-test
  (:require [bluejdbc.core :as jdbc]
            [clojure.test :refer :all]))

(deftest instance-test
  (let [m (assoc (jdbc/instance :wow {:a 100}) :b 200)]
    (is (= {:a 100, :b 200}
           m))

    #_(testing "instance?"
      (is (jdbc/instance? m))
      (is (not (jdbc/instance? {:a 100}))))

    (testing "original/changes"
      (is (= {:a 100}
             (jdbc/original m)))
      (is (= {:b 200}
             (jdbc/changes m)))
      (is (= {:a 300, :b 200}
             (jdbc/changes (assoc m :a 300)))))

    (testing "table/with-table"
      (is (= :wow
             (jdbc/table m)))
      (is (= :ok
             (jdbc/table (jdbc/with-table m :ok)))))))

(deftest equality-test
  (testing "equality"
    (testing "two instances with the same Table should be equal"
      (is (= (jdbc/instance :wow {:a 100})
             (jdbc/instance :wow {:a 100}))))
    (testing "Two instances with different Tables are not equal"
      (is (not= (jdbc/instance :other {:a 100})
                (jdbc/instance :wow {:a 100}))))
    (testing "An Instance should be considered equal to a plain map for convenience purposes"
      (is (= {:a 100}
             (jdbc/instance :wow {:a 100}))))))
