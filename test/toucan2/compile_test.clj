(ns toucan2.compile-test
  (:require
   [clojure.test :refer :all]
   [toucan2.compile :as compile]))

(deftest ^:parallel compile-honeysql-test
  (compile/with-compiled-query [query {:select [:*]
                                       :from   [[:people]]
                                       :where  [:= :id 1]}]
    (is (= ["SELECT * FROM people WHERE id = ?" 1]
           query)))
  (testing "Options"
    (binding [compile/*honeysql-options* {:quoted true}]
      (compile/with-compiled-query [query {:select [:*]
                                           :from   [[:people]]
                                           :where  [:= :id 1]}]
        (is (= ["SELECT * FROM \"people\" WHERE \"id\" = ?" 1]
               query))))))

(deftest ^:parallel condition->honeysql-where-clause-test
  (doseq [[[k v] expected] {[:id :id]           [:= :id :id]
                            [1 :id]             [:= 1 :id]
                            [:id 1]             [:= :id 1]
                            [:a 1]              [:= :a 1]
                            [:id [:> 1]]        [:> :id 1]
                            [:a [:between 1 2]] [:between :a 1 2]}]
    (testing (pr-str `(compile/condition->honeysql-where-clause ~k ~v))
      (is (= expected
             (compile/condition->honeysql-where-clause k v))))))
