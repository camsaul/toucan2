(ns bluejdbc.row-test
  (:require [bluejdbc.row :as row]
            [clojure.test :refer :all]))

(deftest cache-results-test
  (testing "row should only call a column thunk the first time the column value is fetched"
    (let [calls      (atom 0)
          col->thunk {:id (fn []
                            (swap! calls inc))}
          row        (row/row col->thunk)]
      (is (= 1 (get row :id)))
      (is (= 1 (get row :id)))
      (is (= 1 (get row :id))))))
