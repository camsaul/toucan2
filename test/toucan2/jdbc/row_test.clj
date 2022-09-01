(ns toucan2.jdbc.row-test
  (:require [clojure.test :refer :all]
            [toucan2.jdbc.row :as jdbc.row]
            #_[toucan2.result-row :as result-row]))

(deftest ^:parallel row-test
  (testing "Basic map operations on a row"
    (let [c-realized? (atom false)
          col->thunk  {:a (constantly 100)
                       :c (fn [] (reset! c-realized? true) nil)}
          row         (jdbc.row/row col->thunk)]
      (is (= 100
             (:a row)))
      (is (= nil
             (:b row)))
      (is (= #{:a :c}
             (set (keys row))))
      (is (= 2
             (count row)))
      (is (= false
             @c-realized?))
      #_(testing "thunks"
        (is (= (.col-name->thunk ^toucan2.jdbc.row.Row row)
               (result-row/thunks row)))))))

(deftest ^:parallel cache-results-test
  (testing "row should only call a column thunk the first time the column value is fetched"
    (let [calls      (atom 0)
          col->thunk {:id (fn []
                            (swap! calls inc))}
          row        (jdbc.row/row col->thunk)]
      (is (= 1 (get row :id)))
      (is (= 1 (get row :id)))
      (is (= 1 (get row :id))))))
