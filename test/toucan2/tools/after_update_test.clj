(ns toucan2.tools.after-update-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-update :as after-update]
   [toucan2.update :as update]))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(derive ::venues.after-update ::test/venues)

(after-update/define-after-update ::venues.after-update
  [venue]
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj (dissoc venue :created-at :updated-at)))
  nil)

(deftest after-update-test
  (doseq [f [#'update/update!
             #'update/update-returning-pks!]]
    (testing f
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (is (= (condp = f
                   #'update/update!               2
                   #'update/update-returning-pks! [1 2])
                 (f ::venues.after-update :category "bar" {:category "BARRR"})))
          (testing "rows should be updated in DB"
            (is (= [(instance/instance ::venues.after-update {:id 1, :name "Tempest", :category "BARRR"})
                    (instance/instance ::venues.after-update {:id 2, :name "Ho's Tavern", :category "BARRR"})]
                   (select/select [::venues.after-update :id :name :category] :category "BARRR" {:order-by [[:id :asc]]}))))
          (testing (str "rows should have been added to " `*venues-awaiting-moderation*)
            (is (= [(instance/instance ::venues.after-update {:id 1, :name "Tempest", :category "BARRR"})
                    (instance/instance ::venues.after-update {:id 2, :name "Ho's Tavern", :category "BARRR"})]
                   @*venues-awaiting-moderation*))))))))

(derive ::venues.after-update.composed ::venues.after-update)

(def ^:private ^:dynamic *recently-updated-venues* (atom []))

(after-update/define-after-update ::venues.after-update.composed
  [venue]
  (when *recently-updated-venues*
    (swap! *recently-updated-venues* conj (select-keys venue [:id :name])))
  venue)

(deftest compose-test
  (testing "after-update should compose"
    (test/with-discarded-table-changes :venues
      (binding [*venues-awaiting-moderation* (atom [])
                *recently-updated-venues*    (atom [])]
        (is (= 2
               (update/update! ::venues.after-update.composed :category "bar" {:category "BARRR"})))
        (testing (str '*venues-awaiting-moderation* " from " ::venues.after-update)
          (is (= [(instance/instance ::venues.after-update.composed {:id 1, :name "Tempest", :category "BARRR"})
                  (instance/instance ::venues.after-update.composed {:id 2, :name "Ho's Tavern", :category "BARRR"})]
                 @*venues-awaiting-moderation*)))
        (testing (str '*recently-updated-venues* " from " ::venues.after-update.composed)
          (is (= [(instance/instance ::venues.after-update.composed {:id 1, :name "Tempest"})
                  (instance/instance ::venues.after-update.composed {:id 2, :name "Ho's Tavern"})]
                 @*recently-updated-venues*)))))))
