(ns toucan2.compile-test
  (:require
   [clojure.test :refer :all]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.test :as test]))

(deftest compile-honeysql-test
  (conn/with-connection [conn ::test/db]
    (compile/with-compiled-query [query [conn {:select [:*]
                                               :from   [[:people]]
                                               :where  [:= :id 1]}]]
      (is (= ["SELECT * FROM people WHERE id = ?" 1]
             query)))))
