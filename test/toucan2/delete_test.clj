(ns toucan2.delete-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.select :as select]
   [toucan2.test :as test]))

(deftest parse-args-test
  (is (= {:query 1}
         (delete/parse-args nil [1]))))

(deftest row-by-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 3
           (select/count ::test/venues)))
    (is (= 1
           (delete/delete! ::test/venues 1)))
    (is (= []
           (select/select ::test/venues 1)))
    (is (= 2
           (select/count ::test/venues)))
    (is (= #{2}
           (select/select-fn-set :id ::test/venues 2)))))

(derive ::venues.composite-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.composite-pk
  [_model]
  [:id :name])

(deftest row-by-composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::venues.composite-pk :toucan/pk [1 "Tempest"])))
    (is (= []
           (select/select ::test/venues :id 1)))))

(deftest row-by-kv-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::test/venues :name "Tempest")))
    (is (= []
           (select/select ::test/venues :id 1)))))

(deftest multiple-rows-by-kv-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 2
           (delete/delete! ::test/venues :category "bar")))
    (is (= #{"store"}
           (select/select-fn-set :category ::test/venues)))))

(deftest toucan-style-kv-conditions-test
  (testing "Toucan-style fn-args vector"
    (test/with-discarded-table-changes :venues
      (is (= 2
             (delete/delete! ::test/venues :id [:> 1]))))))

(deftest honeysql-query-test
  (test/with-discarded-table-changes :venues
    (is (= 2
           (delete/delete! ::test/venues {:where [:> :id 1]})))))

(derive ::venues.custom-honeysql ::test/venues)

;; TODO
#_(deftest custom-honeysql-test
    (testing "Delete row by PK"
      (test/with-discarded-table-changes :venues
        (is (= ["DELETE FROM venues WHERE id = ?::integer" "1"]
               (query/compile
                (delete/delete! ::venues.custom-honeysql "1"))))
        (is (= 1
               (delete/delete! ::venues.custom-honeysql "1")))
        (is (= []
               (select/select ::test/venues 1)))
        (is (= #{2}
               (select/select-fn-set :id ::test/venues 2)))))
    (testing "Delete row by key-value conditions"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (delete/delete! ::venues.custom-honeysql :id "1")))
        (is (= []
               (select/select ::test/venues :id 1))))
      (testing "Toucan-style fn-args vector"
        (test/with-discarded-table-changes :venues
          (is (= 1
                 (delete/delete! ::venues.custom-honeysql :id [:in ["1"]])))
          (is (= []
                 (select/select ::test/venues :id 1)))))))
