(ns bluejdbc.connectable.current-test
  (:require [bluejdbc.connectable.current :as conn.current]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(m/defmethod conn.current/default-connectable-for-tableable* ::venues
  [_ options]
  (:connectable options))

(deftest current-connectable-test
  (is (= ::wow
         (conn.current/current-connectable ::venues {:connectable ::wow})))
  (testing "Should not override conn.current/*current-connectable* if it is non-default"
    (binding [conn.current/*current-connectable* ::amazing]
      (is (= ::amazing
             (conn.current/current-connectable ::venues {:connectable ::wow}))))))
