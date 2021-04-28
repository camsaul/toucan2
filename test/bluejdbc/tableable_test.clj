(ns bluejdbc.tableable-test
  (:require [bluejdbc.tableable :as tableable]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(deftest string-test
  (is (= "my_table"
         (tableable/table-name "my_table"))))

(deftest keyword-test
  (is (= "user"
         (tableable/table-name :user)))
  (is (= "user"
         (tableable/table-name :user/with-password))))

(m/defmethod tableable/table-name* [:default ::test]
  [connectable _ options]
  (tableable/table-name* connectable :user/with-password options))

(deftest custom-test
  (is (= "user"
         (tableable/table-name ::test))))
