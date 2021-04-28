(ns bluejdbc.util-test
  (:require [bluejdbc.util :as u]
            [clojure.test :refer :all]))

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

(deftest qualified-name-test
  (doseq [[k expected] {:keyword                          "keyword"
                        :namespace/keyword                "namespace/keyword"
                        ;; `qualified-name` should return strings as-is
                        "some string"                     "some string"
                        ;; `qualified-name` should work on anything that implements `clojure.lang.Named`
                        (reify clojure.lang.Named
                          (getName [_] "name")
                          (getNamespace [_] "namespace")) "namespace/name"
                        ;; `qualified-name` shouldn't throw an NPE (unlike `name`)
                        nil                               nil}]
    (testing k
      (is (= expected
             (u/qualified-name k)))))
  (testing "we shouldn't ignore non-nil values -- `u/qualified-name` should throw an Exception if `name` would"
    (is (thrown? ClassCastException
                 (u/qualified-name false)))))
