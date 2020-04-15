(ns bluejdbc.util-test
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

(deftest parse-currency-test
  (doseq [[s expected] {nil             nil
                        ""              nil
                        "   "           nil
                        "$1,000"        1000.0M
                        "$1,000,000"    1000000.0M
                        "$1,000.00"     1000.0M
                        "€1.000"        1000.0M
                        "€1.000,00"     1000.0M
                        "€1.000.000,00" 1000000.0M
                        "-£127.54"      -127.54M
                        "-127,54 €"     -127.54M
                        "kr-127,54"     -127.54M
                        "€ 127,54-"     -127.54M
                        "¥200"          200.0M
                        "¥200."         200.0M
                        "$.05"          0.05M
                        "0.05"          0.05M}]
    (testing (pr-str (list 'parse-currency s))
      (is (= expected
             (u/parse-currency s))))))
