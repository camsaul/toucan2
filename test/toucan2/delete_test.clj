(ns toucan2.delete-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.delete :as delete]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]))

(deftest ^:parallel parse-args-test
  ;; these are basically the same as the select args so we don't need a ton of coverage here.
  (are [args expected] (= expected
                          (query/parse-args ::delete args))
    [:model 1] {:modelable :model, :queryable 1}))

(deftest ^:synchronized row-by-pk-test
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

(deftest ^:synchronized row-by-composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::venues.composite-pk :toucan/pk [1 "Tempest"])))
    (is (= []
           (select/select ::test/venues :id 1)))))

(deftest ^:synchronized row-by-kv-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::test/venues :name "Tempest")))
    (is (= []
           (select/select ::test/venues :id 1)))))

(deftest ^:synchronized multiple-rows-by-kv-conditions-test
  (test/with-discarded-table-changes :venues
    (is (= 2
           (delete/delete! ::test/venues :category "bar")))
    (is (= #{"store"}
           (select/select-fn-set :category ::test/venues)))))

(deftest ^:synchronized toucan-style-kv-conditions-test
  (testing "Toucan-style fn-args vector"
    (test/with-discarded-table-changes :venues
      (is (= 2
             (delete/delete! ::test/venues :id [:> 1]))))))

(deftest ^:synchronized honeysql-query-test
  (test/with-discarded-table-changes :venues
    (is (= 2
           (delete/delete! ::test/venues {:where [:> :id 1]})))))

(deftest ^:synchronized delete-nil-test
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

(deftest ^:synchronized namespaced-test
  (test/with-discarded-table-changes :venues
    (is (= 1
           (delete/delete! ::venues.namespaced :venue/id 3)))
    (is (= nil
           (select/select-one [::test/venues :id :name :category] :id 3)))))

(deftest ^:synchronized positional-connectable-test
  (testing "Support :conn positional connectable arg"
    (test/with-discarded-table-changes :venues
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No default Toucan connection defined"
           (delete/delete! :venues 1)))
      (is (= 1
             (delete/delete! :conn ::test/db :venues 1)))
      (testing "nil :conn should not override current connectable"
        (binding [conn/*current-connectable* ::test/db]
          (is (= 1
                 (delete/delete! :conn nil :venues 2)))))
      (testing "Explicit connectable should override current connectable"
        (binding [conn/*current-connectable* :fake-db]
          (is (= 1
                 (delete/delete! :conn ::test/db :venues 3)))))
      (testing "Explicit connectable should override model default connectable"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Don't know how to get a connection from .* :fake-db"
             (delete/delete! :conn :fake-db ::test/venues 3)))))))
