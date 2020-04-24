(ns bluejdbc.util.macros.enum-map-test
  (:require [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]))

(deftest enum-test
  (is (= 4
         (types/type :integer)
         (types/type :type/integer)
         (types/type 4))))

(deftest reverse-lookup-test
  (is (= :type/integer
         (u/reverse-lookup types/type 4)
         (u/reverse-lookup types/type :integer)
         (u/reverse-lookup types/type :type/integer)))

  (testing "If a key is not found for reverse lookup, should be returned as-is"
    (is (= 100
           (u/reverse-lookup types/type 100)))))
