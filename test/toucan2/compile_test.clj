(ns toucan2.compile-test
  (:require
   [clojure.test :refer :all]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.test :as test]))

(deftest compile-honeysql-test
  (conn/with-connection [conn ::test/db]
    (doseq [conn [conn :default]]
      (testing (format "Connection = %s" (pr-str conn))
        (compile/with-compiled-query [query [conn {:select [:*]
                                                   :from   [[:people]]
                                                   :where  [:= :id 1]}]]
          (is (= ["SELECT * FROM people WHERE id = ?" 1]
                 query)))
        (testing "Options"
          (binding [compile/*honeysql-options* {:quoted true}]
            (compile/with-compiled-query [query [conn
                                                 {:select [:*]
                                                  :from   [[:people]]
                                                  :where  [:= :id 1]}]]
              (is (= ["SELECT * FROM \"people\" WHERE \"id\" = ?" 1]
                     query)))))))))
