(ns toucan2.connection-test
  "Tests for the default JDBC implementation live in [[toucan2.jdbc.connection-test]]."
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]))

(deftest ^:parallel connection-string-protocol-test
  (doseq [[s expected] {"jdbc:postgres://localhost:5432/my_db" "jdbc"
                        "jdbc:" "jdbc"
                        "jdbc" nil
                        "" nil
                        nil nil}]
    (testing (pr-str `(conn/connection-string-protocol ~s))
      (is (= expected
             (conn/connection-string-protocol s))))))

(m/defmethod conn/do-with-connection ::shared-test-connectable
  [_connectable f]
  (f (Object.)))

(m/defmethod conn/do-with-connection ::unshared-test-connectable
  [_connectable f]
  (f (conn/unshared-connection! (Object.))))

(deftest ^:parallel unshared-connection-not-bound-as-current-connectable-test
  (testing "a normal connection is bound as *current-connectable* while in use"
    (conn/do-with-connection
     ::shared-test-connectable
     (fn [conn]
       (is (identical? conn conn/*current-connectable*)))))
  (testing "an unshared connection is handed to f but never bound; ambient resolution is suppressed entirely"
    (binding [conn/*current-connectable* ::previous]
      (conn/do-with-connection
       ::unshared-test-connectable
       (fn [conn]
         (is (conn/unshared-connection? conn))
         (testing "*current-connectable* is nil, shadowing any outer binding, so ambient use falls to :default"
           (is (nil? conn/*current-connectable*))))))))
