(ns toucan2.connectable-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]
            [potemkin :as p]
            [toucan2.connectable :as conn]
            [toucan2.connectable.current :as conn.current]
            [toucan2.mutative :as mutative]
            [toucan2.query :as query]
            [toucan2.select :as select]
            [toucan2.test :as test]
            [toucan2.util :as u]))

(use-fixtures :once test/do-with-test-data)

(deftest parse-with-connection-arg-test
  (is (= {:binding '_, :connectable nil}
         (#'conn/parse-with-connection-arg '[_])))
  (is (= {:connectable nil, :binding '_}
         (#'conn/parse-with-connection-arg '[])))
  (is (= {:binding '_, :connectable nil}
         (#'conn/parse-with-connection-arg '[_ _])))
  (is (= {:binding '_, :connectable nil}
         (#'conn/parse-with-connection-arg '[_ nil])))
  (is (= {:binding '_, :connectable nil}
         (#'conn/parse-with-connection-arg '[nil])))
  (is (= {:connectable nil, :binding '_}
         (#'conn/parse-with-connection-arg nil)))
  (is (= {:connectable nil, :binding '_}
         (#'conn/parse-with-connection-arg '_)))
  (is (= '{:binding a, :connectable b, :options c}
         (#'conn/parse-with-connection-arg '[a b c])))
  (is (= '{:binding a, :connectable b, :tableable c, :options d}
         (#'conn/parse-with-connection-arg '[a b c d]))))

(p/defrecord+ ^:private MockConnection [connectable options closed?]
  java.sql.Connection
  (close [_]
    (assert (not @closed?))
    (reset! closed? true)))

(m/defmethod conn/connection* ::mock-connection
  [connectable options]
  {:connection (->MockConnection connectable options (atom false))
   :new?       true})

(m/defmethod conn.current/default-options-for-connectable* ::mock-connection
  [_]
  {:default-options true})

(derive ::mock-connection-additional-options ::mock-connection)

(m/defmethod conn.current/default-options-for-connectable* ::mock-connection-additional-options
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
      (is (identical? conn-1 conn.current/*current-connection*))
      (is (= ::mock-connection
             conn.current/*current-connectable*)))
    (is (not @(:closed? conn-1)))
    (conn/with-connection [conn-2 conn.current/*current-connectable*]
      (is (instance? java.sql.Connection conn-2))
      (testing "Should not create a new Connection"
        (is (identical? conn-1 conn-2))))
    (testing "Connection should not get closed if it was not newly created"
      (is (not @(:closed? conn-1))))))

(deftest default-connection-test
  (try
    (m/add-primary-method! conn/connection* :toucan2/default (fn [_ _ options]
                                                                (conn/connection* ::mock-connection options)))
    (conn/with-connection [conn]
      (is (instance? java.sql.Connection conn))
      (is (= ::mock-connection
             (:connectable conn))))
    (finally
      (m/remove-primary-method! conn/connection* :toucan2/default))))

(deftest with-transaction-test
  (test/with-venues-reset
    (test/with-default-connection
      (testing "should commit if no exception is thrown"
        (testing "_ = use current connection"
          (is (= 1
                 (conn/with-transaction _
                   (mutative/insert! :venues {:name "Venue 4", :category "place"})))))
        (is (select/exists? :venues :name "Venue 4"))))

    (testing "should rollback if exception is thrown"
      (is (thrown-with-msg?
           Exception
           #"Oops"
           (conn/with-transaction :test/postgres
             (mutative/insert! :venues :name "Venue 5", :category "place")
             (mutative/insert! :venues :name "Venue 6", :category "place")
             (testing "uncommitted objects should be visible inside transaction"
               (is (select/exists? [:test/postgres :venues] :name "Venue 5"))
               (is (select/exists? [:test/postgres :venues] :name "Venue 6")))
             (throw (Exception. "Oops!")))))
      (is (not (select/exists? [:test/postgres :venues] :name "Venue 5")))
      (is (not (select/exists? [:test/postgres :venues] :name "Venue 6"))))

    (testing "conn binding -- should not create a new Connection"
      (test/with-default-connection
        (conn/with-connection [conn]
          (conn/with-transaction [t-conn]
            (is (identical? conn t-conn))
            (testing "nested transactions"
              (conn/with-transaction [t-conn-2]
                (is (identical? conn t-conn-2)))))
          (conn/with-transaction [t-conn _]
            (is (identical? conn t-conn)))
          (conn/with-transaction [t-conn conn]
            (is (identical? conn t-conn))))))))

(deftest nested-transaction-test
  (test/with-venues-reset
    (test/with-default-connection
      (letfn [(venue-visible? [id]
                (select/exists? :venues :name (format "Venue %d" id)))
              (venue-visible-outside? []
                (select/exists? [:test/postgres :venues] :name "Venue 6"))
              (f [top-level-fails?]
                (conn/with-transaction [t-conn]
                  (mutative/insert! :venues :name "Venue 6", :category "place")
                  (testing "uncommitted object"
                    (testing "should be visible inside transaction"
                      (is (venue-visible? 6)))
                    (testing "should not be visible outside transaction"
                      (is (not (venue-visible-outside?)))))

                  (testing "nested transaction that commits\n"
                    (conn/with-transaction _
                      (mutative/insert! :venues :name "Venue 7", :category "place"))
                    (testing "committing a nested transaction should not commit its parent transaction"
                      (is (venue-visible? 6))
                      (is (not (venue-visible-outside?))))
                    (testing "object from committed nested transaction"
                      (testing "should be visible inside parent transaction"
                        (is (venue-visible? 7)))
                      (testing "should not be visible outside"
                        (is (not (select/exists? [:test/postgres :venues] :name "Venue 7"))))))

                  (testing "nested transaction that throws an Exception\n"
                    (is (thrown-with-msg?
                         Exception
                         #"Oops"
                         (conn/with-transaction _
                           (is (venue-visible? 6))
                           (testing "Object created in parent transaction (not committed) should be visible"
                             (mutative/insert! :venues :name "Venue 8", :category "place"))
                           (throw (Exception. "Oops!")))))
                    (testing "Object from top-level transaction should still exist"
                      (is (venue-visible? 6)))
                    (testing "Object from nested transaction that threw an Exception should have been rolled back"
                      (is (not (venue-visible? 8))))
                    (when top-level-fails?
                      (throw (Exception. "Oh no!"))))))]
        (testing "top-level transaction fails"
          (is (thrown-with-msg?
               Exception
               #"Oh no"
               (f true)))
          (testing "after roll back:"
            (testing "object inserted at top-level should not exist"
              (is (not (venue-visible? 6))))
            (testing "object inserted by nested transaction (committed) should not exist"
              (is (not (venue-visible? 7))))
            (testing "object inserted by nested transaction (failed) should not exist"
              (is (not (venue-visible? 8))))))

        (testing "top-level transaction succeeds"
          (f false)
          (testing "object inserted at top-level should exist"
            (is (venue-visible? 6)))
          (testing "object inserted by nested transaction (committed) should exist"
            (is (venue-visible? 7)))
          (testing "object inserted by nested transaction (failed) should not exist"
            (is (not (venue-visible? 8)))))))))

(m/defmethod conn.current/default-connectable-for-tableable* ::venues
  [_ options]
  (:connectable options))

(deftest default-connectable-for-tableable-test
  (conn/with-connection [conn nil ::venues {:connectable ::mock-connection}]
    (is (instance? MockConnection conn))
    (is (= {:connectable ::mock-connection
            :options     {:default-options true
                          :connectable     ::mock-connection}}
           (dissoc conn :closed?)))))

(deftest dispatch-on-test
  (is (= [{:id 1, :name "Cam", :created-at (t/offset-date-time "2020-04-21T23:56Z")}]
         (query/query (u/dispatch-on test/test-postgres-url :toucan2.jdbc/postgresql) "SELECT * FROM PEOPLE WHERE id = 1;"))))
