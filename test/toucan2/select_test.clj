(ns toucan2.select-test
  (:require
   [clojure.test :refer :all]
   [toucan2.select :as select]
   [toucan2.test :as test]))

(deftest ^:parallel parse-select-args-test
  (doseq [[args expected] {[1]
                           {:query 1}

                           [:id 1]
                           {:conditions {:id 1}, :query {}}

                           [{:where [:= :id 1]}]
                           {:query {:where [:= :id 1]}}

                           [:name "Cam" {:where [:= :id 1]}]
                           {:conditions {:name "Cam"}, :query {:where [:= :id 1]}}

                           [::my-query]
                           {:query ::my-query}

                           [::my-query {:options? true}]
                           {:query ::my-query, :options {:options? true}}}]
    (testing `(select/parse-select-args ~args)
      (is (= expected
             (select/parse-select-args args))))))

(deftest select-test
  (let [expected [(list 'magic-map ::test/people {:id 1, :name "Cam", :created-at #inst "2020-04-21T23:56:00.000000000-00:00"})]]
    (testing "plain SQL"
      (is (= expected
             (select/select ::test/people "SELECT * FROM people WHERE id = 1;"))))
    (testing "sql-args"
      (is (= expected
             (select/select ::test/people ["SELECT * FROM people WHERE id = ?;" 1]))))
    (testing "HoneySQL"
      (is (= expected
             (select/select ::test/people {:select [:*], :from [[:people]], :where [:= :id 1]}))))
    (testing "PK"
      (is (= expected
             (select/select ::test/people 1))))
    (testing "conditions"
      (is (= expected
             (select/select ::test/people :id 1))))))
