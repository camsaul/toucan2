(ns toucan2.util-test
  (:require [toucan2.util :as u]
            [clojure.test :refer :all]
            [clojure.string :as str]))

(defmacro ^:private out-str-lines [& body]
  `(some-> (with-out-str ~@body)
           str/trim
           not-empty
           str/split-lines))

(defmacro ^:private println-debug [& args])

(deftest println-debug-test
  (testing "debugging disabled"
    (let [evaled? (atom false)]
      (testing "don't print anything"
        (is (= nil
               (out-str-lines (u/println-debug (do (reset! evaled? true) "wow"))))))
      (testing "don't eval args"
        (is (= false
               @evaled?)))))
  (testing "debugging enabled"
    (binding [u/*debug* true]
      (let [evaled? (atom false)]
        (is (= ["wow"]
               (out-str-lines (u/println-debug (do (reset! evaled? true) "wow")))))
        (is (= true
               @evaled?))))))

(deftest with-debug-result-test
  (testing "debugging disabled"
    (let [message-form-evaled? (atom false)]
      (testing "don't print anything"
        (is (= nil
               (out-str-lines
                (u/with-debug-result (do
                                       (reset! message-form-evaled? true)
                                       "DEBUG")
                  {:cans (+ 1 1)})))))
      (testing "don't eval message form"
        (is (= false
               @message-form-evaled?)))))
  (testing "debugging enabled"
    (binding [u/*debug* true]
      (let [message-form-evaled? (atom false)]
        (is (= ["DEBUG"
                "+-> {:cans 2}"]
               (out-str-lines
                (u/with-debug-result (do
                                       (reset! message-form-evaled? true)
                                       "DEBUG")
                  {:cans (+ 1 1)}))))
        (is (= true
               @message-form-evaled?)))))
  (testing "Nested debug results"
    (binding [u/*debug* true]
      (is (= ["A"
              "|  B"
              "|  +-> 2"
              "|  C"
              "|  +-> :cans"
              "+-> [2 :cans]"]
             (out-str-lines
              (u/with-debug-result "A"
                [(u/with-debug-result "B"
                   2)
                 (u/with-debug-result "C"
                   :cans)])))))))
