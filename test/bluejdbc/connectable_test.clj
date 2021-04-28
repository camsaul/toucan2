(ns bluejdbc.connectable-test
  (:require [bluejdbc.connectable :as conn]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [methodical.core :as m]
            [potemkin :as p]))

(comment test/keep-me)

(p/defrecord+ ^:private MockConnection [connectable options closed?]
  java.sql.Connection
  (close [_]
    (assert (not @closed?))
    (reset! closed? true)))

(m/defmethod conn/connection* ::mock-connection
  [connectable options]
  {:connection (->MockConnection connectable options (atom false))
   :new?       true})

(m/defmethod conn/default-options ::mock-connection
  [_]
  {:default-options true})

(derive ::mock-connection-additional-options ::mock-connection)

(m/defmethod conn/default-options ::mock-connection-additional-options
  [connectable]
  (merge (next-method connectable)
         {:additional-options true}))

(deftest options-test
  (testing "Connection should get both the default options and anything passed to `with-connection` during creation"
    (conn/with-connection [conn ::mock-connection {:more-options true}]
      (is (instance? java.sql.Connection conn))
      (is (= {:connectable ::mock-connection
              :options     {:default-options true
                            :more-options    true}}
             (dissoc conn :closed?)))))
  (testing "Should be able to get even more options by taking advantage of the keyword hierarchy and Methodical `next-method`"
    (conn/with-connection [conn ::mock-connection-additional-options {:more-options true}]
      (is (instance? java.sql.Connection conn))
      (is (= {:default-options    true
              :additional-options true
              :more-options       true}
             (:options conn))))))

(deftest current-connection-test
  (conn/with-connection [conn-1 ::mock-connection]
    (testing "Should bind dynamic variables"
      (is (instance? java.sql.Connection conn-1))
      (is (identical? conn-1 conn/*connection*))
      (is (= ::mock-connection
             conn/*connectable*)))
    (is (not @(:closed? conn-1)))
    (conn/with-connection [conn-2 conn/*connectable*]
      (is (instance? java.sql.Connection conn-2))
      (testing "Should not create a new Connection"
        (is (identical? conn-1 conn-2))))
    (testing "Connection should not get closed if it was not newly created"
      (is (not @(:closed? conn-1))))))

;; TODO - `:default-connection` test
