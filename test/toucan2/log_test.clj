(ns toucan2.log-test
  (:require
   [clojure.test :refer :all]
   [pretty.core :as pretty]
   [toucan2.log :as log]))

(set! *warn-on-reflection* true)

(deftest env-var-topics-test
  (are [s expected] (= expected
                       (#'log/env-var-topics s))
    nil       nil
    ""        nil
    "abc"     #{:abc}
    "abc,def" #{:abc :def}))

(defrecord Reducible [is-realized?]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reset! is-realized? true)
    (reduce rf init [])))

(deftest pprint-doc-test
  (testing "Pretty-printing an clojure.lang.IReduceInit should not realize it. Top-level object:"
    (binding [log/*color* false]
      (doseq [f [identity
                 list
                 vector
                 #(eduction identity %)
                 (fn [x] {:x x})
                 (fn [x]
                   (reify
                     pretty/PrettyPrintable
                     (pretty [_this]
                       (list 'reducible x))))]]
        (let [is-realized? (atom false)
              reducible    (->Reducible is-realized?)
              x            (log/->Doc [(f reducible)])]
          (testing (.getCanonicalName (class x))
            (is (string? (#'log/pprint-doc-to-str x)))
            (is (false? @is-realized?))))))))

(deftest interleave-all-test
  (are [x y expected] (= expected
                         (#'log/interleave-all x y))
    [1 2 3] [:a :b :c] [1 :a 2 :b 3 :c]
    [1 2 3] [:a]       [1 :a 2 3]
    [1 2 3] []         [1 2 3]
    [1]     [:a :b :c] [1 :a :b :c]
    []      [:a :b :c] [:a :b :c]))

(deftest build-doc-test
  (testing "Text after the last argument should be included"
    (is (= `(log/->Doc [(log/->Text "") ~'x (log/->Text "") ~'y (log/->Text " returning instances")])
           (#'log/build-doc "%s %s returning instances" 'x 'y)))))

(deftest enable-level-test
  (binding [log/*level* :info]
    (are [level expected] (= expected
                             (log/enable-level? level))
      :error true
      :warn  true
      :info  true
      :debug false
      :trace false
      nil    false
      :fake  false)))

(deftest log-macro-validation-test
  (testing "valid forms"
    (is (some? (macroexpand `(log/infof :compile "VERY NICE MESSAGE %s" 100))))
    (is (some? (macroexpand `(log/infof :compile ~'e "VERY NICE MESSAGE %s" 100)))))
  (testing "wrong number of args"
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand `(log/infof :compile "VERY NICE MESSAGE %s"))))
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand `(log/infof :compile "VERY NICE MESSAGE %s" 100 200)))))
  (testing "invalid topic"
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand `(log/infof :fake-topic "VERY NICE MESSAGE %s")))))
  (testing "second arg is not a format string"
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand `(log/infof :compile ~'e 100 "VERY NICE MESSAGE %s")))))
  (testing "don't use pr-str inside log calls"
    (is (thrown?
         clojure.lang.Compiler$CompilerException
         (macroexpand (list `log/infof :compile "VERY NICE MESSAGE %s" '(pr-str 100)))))))
