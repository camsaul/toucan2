(ns bluejdbc.compile-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(deftest honeysql-options-test
  (testing "Make sure HoneySQL options are applied"
    (let [options {:honeysql/quoting             :ansi
                   :honeysql/allow-dashed-names? true}]
      (jdbc/with-connection [conn (test/connection) options]
        (is (= ["SELECT ? AS \"my-date\""
                (t/offset-date-time "2020-04-15T07:04:02.465161Z")]
               (jdbc/compile conn {:select [[(t/offset-date-time "2020-04-15T07:04:02.465161Z") :my-date]]} options)))))))
