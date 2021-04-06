(ns bluejdbc.compile-test
  (:require [bluejdbc.core :as bluejdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [java-time :as t]))

(deftest honeysql-options-test
  (testing "Make sure HoneySQL options are applied"
    (let [options {:honeysql {:quoting             :ansi
                              :allow-dashed-names? true}}]
      (test/with-every-test-connectable [connectable]
        (is (= ["SELECT ? AS \"my-date\""
                (t/offset-date-time "2020-04-15T07:04:02.465161Z")]
               (bluejdbc/compile connectable {:select [[(t/offset-date-time "2020-04-15T07:04:02.465161Z") :my-date]]}
                                 options)))))))
