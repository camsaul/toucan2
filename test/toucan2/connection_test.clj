(ns toucan2.connection-test
  "Tests for the default JDBC implementation live in [[toucan2.jdbc.connection-test]]."
  (:require
   [clojure.test :refer :all]
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
