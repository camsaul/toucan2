(ns bluejdbc.connectable-test
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.test :as test]
            [bluejdbc.util :as u]
            [clojure.test :refer :all]
            [next.jdbc.result-set :as jdbc.rs]))

(comment test/keep-me)

(deftest options-test
  (conn/with-connection [[conn options] :test/postgres {:more-options true}]
    (is (instance? java.sql.Connection conn))
    (is (= (merge
            conn/default-options
            {:default-options true
             :more-options    true})
           options))))

(deftest current-connection-test
  (conn/with-connection [[conn-1] :test/postgres {:more-options true}]
    (testing "Should bind dynamic variables"
      (is (identical? conn-1 conn/*connection*))
      (is (= :test/postgres
             conn/*connectable*))
      (is (= (merge
              conn/default-options
              {:default-options true
               :more-options    true})
             conn/*options*)))
    (conn/with-connection [[conn-2 options] conn/*connectable* {:more-options 100, :even-more-options true}]
      (is (instance? java.sql.Connection conn-2))
      (testing "Should not create a new Connection"
        (is (identical? conn-1 conn-2)))
      (is (= (merge
              conn/default-options
              {:default-options   true
               :more-options      100
               :even-more-options true})
             options)))))

;; TODO - `:default-connection` test
