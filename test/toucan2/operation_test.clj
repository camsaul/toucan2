(ns toucan2.operation-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.operation :as op]
   [toucan2.realize :as realize]
   [toucan2.test :as test])
  (:import
   (java.time LocalDateTime)))

(deftest select-reducible-with-pks-test
  (is (= [(instance/instance ::test/venues {:id         1
                                            :name       "Tempest"
                                            :category   "bar"
                                            :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                            :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
         (realize/realize (op/select-reducible-with-pks ::test/venues nil [1]))
         (realize/realize (op/select-reducible-with-pks ::test/venues nil [[1]]))))
  (testing "model-columns; multiple PKs"
    (is (= [(instance/instance ::test/venues {:id 1, :name "Tempest"})
            (instance/instance ::test/venues {:id 2, :name "Ho's Tavern"})]
           (realize/realize (op/select-reducible-with-pks ::test/venues [:id :name] [1 2]))
           (realize/realize (op/select-reducible-with-pks ::test/venues [:id :name] [[1] [2]])))))
  (testing "empty PKs"
    (is (= []
           (realize/realize (op/select-reducible-with-pks ::test/venues [:id :name] []))
           (realize/realize (op/select-reducible-with-pks ::test/venues [:id :name] nil))))))
