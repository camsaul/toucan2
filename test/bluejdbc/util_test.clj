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

(deftest recursive-merge-test
  (is (= {:a 10, :b {:c 2, :d 3}, :d 3}
         (u/recursive-merge {:a 1, :b {:c 2}, :d 3}
                            {:a 10, :b {:d 3}})))
  (testing "recursively nested"
    (is (= {:a {:b {:c 3, :d 2, :e 4}}}
           (u/recursive-merge {:a {:b {:c 1, :d 2}}}
                              {:a {:b {:c 3, :e 4}}}))))
  (testing "vector value"
    (is (= {:a 10, :b {:c 2, :d 3}, :d 3}
           (u/recursive-merge {:a [1], :b {:c 2}, :d 3}
                              {:a 10, :b {:d 3}})))))

(deftest dispatch-value-test
  (is (= clojure.lang.PersistentArrayMap
         (u/dispatch-value (array-map))))
  (is (= :wow
         (u/dispatch-value :wow)))
  (is (= :ok
         (u/dispatch-value (u/dispatch-on :wow :ok))))
  (is (= :birds
         (u/dispatch-value (u/dispatch-on {} :birds))))
  (testing "Should be able to extend protocol via metadata"
    (is (= ::meta
           (u/dispatch-value (with-meta (fn []) {`u/dispatch-value (constantly ::meta)})))))
  (testing "Should dispatch on `:type` if object has it"
    (is (= :wow
           (u/dispatch-value (with-meta 'x {:type :wow}))))))
