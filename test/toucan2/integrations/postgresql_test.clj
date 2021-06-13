(ns toucan2.integrations.postgresql-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [toucan2.query :as query]
            [toucan2.select :as select]
            [toucan2.test :as test]))

(deftest money-test
  (testing "MONEY columns"
    (test/with-default-connection
      (doseq [sql ["DROP TABLE IF EXISTS bird_prices;"
                   "CREATE TABLE bird_prices (id serial PRIMARY KEY NOT NULL, bird text NOT NULL, price money NOT NULL);"
                   "INSERT INTO bird_prices (bird, price)
                  VALUES
                  ('parakeet', '29.99'::money),
                  ('toucan', '10000.0'::money),
                  ('pigeon', '0.0'::money)"]]
        (query/execute! sql))
      (is (= [{:id 1, :bird "parakeet", :price 29.99M}
              {:id 2, :bird "toucan", :price 10000.00M}
              {:id 3, :bird "pigeon", :price 0.00M}]
             (select/select "bird_prices" {:order-by [[:id :asc]]}))))))

(deftest time-test
  (testing "Passing in OffsetTime and returning TIME"
    (test/with-default-connection
      (is (= [{:time (t/local-time "01:48:07.008668")}]
             (query/query ["SELECT ? AS time" (t/offset-time "01:48:07.008668Z")]))))))
