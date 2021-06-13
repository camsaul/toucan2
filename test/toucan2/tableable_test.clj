(ns toucan2.tableable-test
  (:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.tableable :as tableable]))

(deftest string-test
  (is (= "my_table"
         (tableable/table-name "my_table"))))

(deftest keyword-test
  (is (= "user"
         (tableable/table-name :user)))
  ;; (testing "Should let you use namespace-qualified `:toucan2.table/` and return the name"
  ;;   (is (= "user"
  ;;          (tableable/table-name :toucan2.table/user))))
  (testing "Other namespaced keywords should return the namespace by default."
    (is (= "user"
           (tableable/table-name :user/with-password)))))

(m/defmethod tableable/table-name* [:default ::test]
  [connectable _ options]
  (tableable/table-name* connectable :user/with-password options))

(deftest custom-test
  (is (= "user"
         (tableable/table-name ::test))))
