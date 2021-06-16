(ns toucan2.log-test
  (:require [clojure.test :refer :all]
            [toucan2.log :as log]))

(deftest debug-log?-test
  (testing "level = debug"
    (binding [log/*debug-log-level* :debug]
      (is (not (log/debug-log? :trace)))
      (is (log/debug-log? :debug))
      (is (log/debug-log? :info))))
  (testing "level = trace"
    (binding [log/*debug-log-level* :trace]
      (is (log/debug-log? :trace))
      (is (log/debug-log? :debug))
      (is (log/debug-log? :info))))
  (testing "level = nil"
    (binding [log/*debug-log-level* nil]
      (is (not (log/debug-log? :trace)))
      (is (not (log/debug-log? :debug)))
      (is (not (log/debug-log? :info))))))
