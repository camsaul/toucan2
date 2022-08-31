(ns toucan2.delete-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]))

(deftest parse-args-test
  (is (= {:modelable :model, :queryable 1}
         (query/parse-args ::delete [:model 1]))))

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

(deftest delete-nil-test
  (testing "(delete! model nil) should basically be the same as (delete! model :toucan/pk nil)"
    (let [parsed-args (query/parse-args :toucan.query-type/delete.update-count [::test/venues nil])]
      (is (= {:modelable ::test/venues, :queryable nil}
             parsed-args))
      (let [resolved-query (pipeline/resolve-query :toucan.query-type/delete.update-count ::test/venues (:queryable parsed-args))]
        (is (= nil
               resolved-query))
        (is (= {:delete-from [:venues]
                :where       [:= :id nil]}
               (pipeline/build :toucan.query-type/delete.update-count ::test/venues parsed-args resolved-query)))
        (is (= [(case (test/current-db-type)
                  :h2       "DELETE FROM \"VENUES\" WHERE \"ID\" IS NULL"
                  :postgres "DELETE FROM \"venues\" WHERE \"id\" IS NULL")]
               (tools.compile/compile
                 (delete/delete! ::test/venues nil))))))
    (test/with-discarded-table-changes :venues
      (is (= 0
             (delete/delete! ::test/venues nil))))))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::test/venues :venue})

(deftest namespaced-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::venues.namespaced :venue/id 3)))
    (is (= nil
           (select/select-one [::test/venues :id :name :category] :id 3)))))
