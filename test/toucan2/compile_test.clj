(ns toucan2.compile-test
  (:require
   [clojure.test :refer :all]
   [toucan2.compile :as compile]))

(deftest support-destructuring-test
  (compile/with-compiled-query [[query] [nil ["SELECT * FROM people WHERE id = ?" 1]]]
    (is (= "SELECT * FROM people WHERE id = ?"
           query))))

(deftest ^:parallel compile-honeysql-test
  (compile/with-compiled-query [query [nil {:select [:*]
                                            :from   [[:people]]
                                            :where  [:= :id 1]}]]
    (is (= ["SELECT * FROM people WHERE id = ?" 1]
           query)))
  (testing "Options"
    (binding [compile/*honeysql-options* {:quoted true}]
      (compile/with-compiled-query [query [nil {:select [:*]
                                                :from   [[:people]]
                                                :where  [:= :id 1]}]]
        (is (= ["SELECT * FROM \"people\" WHERE \"id\" = ?" 1]
               query))))))
