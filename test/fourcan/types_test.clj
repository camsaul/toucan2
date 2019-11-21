(ns fourcan.types-test
  (:require [clojure.test :refer :all]
            [fourcan.types :as types]))

(deftest query-ops-test
  (testing "equality"
    (is (= (types/->Query :x {:select [:*], :y 1} nil nil)
           (types/->Query :x {:select [:*], :y 1} nil nil)))))

(deftest query-map-opts-test
  (is (= (types/->Query :x {:select [:*], :y 1} nil nil)
         (merge (types/->Query :x {:select [:*]} nil nil) {:y 1}))))

(deftest instance-test
  (is (= {}
         (types/instance ::MyModel)))
  (is (= {}
         (types/instance ::MyModel {})))
  (is (= {:a 1}
         (types/instance ::MyModel {:a 1})))
  (is (= ::MyModel
         (types/model (types/instance ::MyModel))))
  (is (= ::MyModel
         (types/model (types/instance ::MyModel {}))))
  (let [m (types/instance ::MyModel {:original? true})]
    (is (= {:original? false}
           (assoc m :original? false)))
    (is (= {:original? true}
           (.orig m)))
    (is (= {:original? true}
           (.m m)))
    (is (= {:original? true}
           (.orig ^fourcan.types.Instance (assoc m :original? false))))
    (is (= {:original? false}
           (.m ^fourcan.types.Instance (assoc m :original? false))))
    (testing "fetching original value"
      (is (= {:original? true}
             (types/original (assoc m :original? false))))
      (is (= {}
             (dissoc m :original?)))
      (is (= {:original? true}
             (types/original (dissoc m :original?))))
      (is (= nil
             (types/original {})))
      (is (= nil
             (types/original nil))))))
