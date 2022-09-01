(ns toucan2.tools.disallow-test
  (:require
   [clojure.test :refer :all]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.disallow :as disallow]
   [toucan2.update :as update]))

(derive ::venues.no-select ::test/venues)
(derive ::venues.no-select ::disallow/select)

(deftest ^:parallel disallow-select-test
  (is (thrown-with-msg?
       Exception
       #"You cannot select :toucan2.tools.disallow-test/venues.no-select"
       (select/select ::venues.no-select))))

(derive ::venues.no-delete ::test/venues)
(derive ::venues.no-delete ::disallow/delete)

(deftest ^:parallel disallow-delete-test
  (is (thrown-with-msg?
       Exception
       #"You cannot delete instances of :toucan2.tools.disallow-test/venues.no-delete"
       (delete/delete! ::venues.no-delete))))

(derive ::venues.no-insert ::test/venues)
(derive ::venues.no-insert ::disallow/insert)

(deftest ^:parallel disallow-insert-test
  (is (thrown-with-msg?
       Exception
       #"You cannot create new instances of :toucan2.tools.disallow-test/venues.no-insert"
       (insert/insert! ::venues.no-insert {:a 1, :b 2}))))

(derive ::venues.no-update ::test/venues)
(derive ::venues.no-update ::disallow/update)

(deftest ^:parallel disallow-update-test
  (is (thrown-with-msg?
       Exception
       #"You cannot update a :toucan2.tools.disallow-test/venues.no-update after it has been created"
       (update/update! ::venues.no-update 1 {:a 1, :b 2}))))
