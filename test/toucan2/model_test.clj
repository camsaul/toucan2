(ns toucan2.model-test
  (:require
   [clojure.test :refer :all]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(deftest reducible-model-query-test
  (is (= [(list 'magic-map :people {:id 1, :name "Cam", :created-at #inst "2020-04-21T23:56:00.000000000-00:00"})]
         (into [] (model/reducible-model-query ::test/db :people "SELECT * FROM people WHERE id = 1;")))))
