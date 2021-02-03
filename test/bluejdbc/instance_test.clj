(ns bluejdbc.instance-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.instance :as instance]
            [clojure.test :refer :all]))

(deftest instance-test
  (let [m (assoc (jdbc/instance :wow {:a 100}) :b 200)]
    (is (= {:a 100, :b 200}
           m))

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

(deftest instance-test-2
  (is (= {}
         (instance/instance ::MyModel)))
  (is (= {}
         (instance/instance ::MyModel {})))
  (is (= {:a 1}
         (instance/instance ::MyModel {:a 1})))
  (is (= ::MyModel
         (instance/table (instance/instance ::MyModel))))
  (is (= ::MyModel
         (instance/table (instance/instance ::MyModel {}))))
  (let [m (instance/instance ::MyModel {:original? true})]
    (is (= {:original? false}
           (assoc m :original? false)))
    (is (= {:original? true}
           (.orig m)))
    (is (= {:original? true}
           (.m m)))
    (is (= {:original? true}
           (.orig ^bluejdbc.instance.Instance (assoc m :original? false))))
    (is (= {:original? false}
           (.m ^bluejdbc.instance.Instance (assoc m :original? false))))
    (testing "fetching original value"
      (is (= {:original? true}
             (instance/original (assoc m :original? false))))
      (is (= {}
             (dissoc m :original?)))
      (is (= {:original? true}
             (instance/original (dissoc m :original?))))
      (is (= nil
             (instance/original {})))
      (is (= nil
             (instance/original nil))))))
