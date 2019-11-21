(ns fourcan.compile-test
  (:require [clojure
             [string :as str]
             [test :refer :all]]
            [fourcan
             [compile :as compile]
             [debug :as debug]
             [hierarchy :as hierarchy]]
            [methodical.core :as m]))

(m/defmethod compile/table-name ::user [_] :users)

(hierarchy/derive ::user_2 ::user)

(deftest table-name-test
  (testing "no impl should return keywords as-is"
    (is (= nil
           (compile/table-name nil)))
    (is (= :checkins
           (compile/table-name :checkins))))
  (testing "custom implementations of table"
    (is (= :users
           (compile/table-name ::user)))
    (is (= :users
           (compile/table-name ::user_2)))))

(m/defmethod compile/primary-key ::compound-key [_] [:id_1 :id_2])

(deftest primary-key-test
  (is (= :id
         (compile/primary-key nil)
         (compile/primary-key :whatever)))
  (is (= (compile/primary-key ::compound-key)
         [:id_1 :id_2])))

(deftest primary-key-where-clause-test
  (testing "One-column PK"
    (is (= [:= :id 100]
           (compile/primary-key-where-clause nil 100)
           (compile/primary-key-where-clause :whatever 100)
           (compile/primary-key-where-clause nil [100])))
    (is (= [:= :id nil]
           (compile/primary-key-where-clause :whetever nil))
        "nil should be treated as a valid PK"))
  (testing "Compound PK"
    (is (= [:and [:= :id_1 100] [:= :id_2 200]]
           (compile/primary-key-where-clause ::compound-key [100 200]))))
  (testing "Mismatch between defined PK and number of args passed should throw an Exception"
    (is (thrown-with-msg?
         Exception #"model :toucan.models-test/compound-key expected 2 primary key values \[:id_1 :id_2\], got 1 values"
         (compile/primary-key-where-clause ::compound-key [100])))
    (is (thrown-with-msg?
         Exception #"model nil expected 1 primary key values \[:id\], got 0 values"
         (compile/primary-key-where-clause nil [])))
    (is (thrown-with-msg?
         Exception #"model :m expected 1 primary key values \[:id\], got 2 values"
         (compile/primary-key-where-clause :m [100 200])))
    (is (thrown-with-msg?
         Exception #"model :toucan.models-test/compound-key expected 2 primary key values \[:id_1 :id_2\], got 0 values"
         (compile/primary-key-where-clause ::compound-key [])))))


(deftest compile-test
  (testing "basic compilation of HoneySQL forms"
    (is (= ["SELECT * FROM \"my\".\"table\""]
           (compile/compile nil {:select [:*], :from [:my.table]})))
    (is (= ["SELECT * FROM \"my\".\"table\" WHERE \"field\" = ?" 2]
           (compile/compile :model {:select [:*], :from [:my.table] :where [:= :field 2]}))))
  (testing "SQL should be returned as-is, wrapped in a vector if needed"
    (is (= ["SELECT *"]
           (compile/compile nil "SELECT *")))
    (is (= ["SELECT * WHERE x = ?" 100]
           (compile/compile nil ["SELECT * WHERE x = ?" 100]))))
  (testing "with debugging enabled, compilation before/after should be printed"
    (is (= ["(compile :model {:select [:*], :from [:table]})"
            ";; -> [\"SELECT * FROM \\\"table\\\"\"]"]
           (str/split-lines
            (with-out-str
              (debug/debug
                (compile/compile :model {:select [:*], :from [:table]}))))))))

(m/defmethod compile/honeysql-options ::compile
  [_]
  nil)

(m/defmethod compile/compile ::compile-2
  [_ form]
  form)

(deftest custom-compile-test
  (testing "We should be able to override HoneySQL options such as `:quoting` by implementing `honeysql-options`"
    (is (= ["SELECT * FROM my.table"]
           (compile/compile ::compile {:select [:*], :from [:my.table]}))))
  (testing "We should be able to override compilation behavior entirely by implementing `compile`"
    (is (= {:select [:*], :from [:my.table]}
           (compile/compile ::compile-2 {:select [:*], :from [:my.table]}))))
  (testing "debugging should stil work even if overriding `compile`"
    (is (= ["(compile :fourcan.compile-test/compile-2 form)"
            ";; -> form"]
           (str/split-lines (with-out-str (debug/debug (compile/compile ::compile-2 'form))))))))
