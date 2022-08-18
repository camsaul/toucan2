(ns toucan2.tools.after-update-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-update :as after-update]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(derive ::venues.after-update ::test/venues)

(after-update/define-after-update ::venues.after-update
  [venue]
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj venue))
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
            (is (= [(instance/instance ::venues.after-update {:id         1
                                                              :name       "Tempest"
                                                              :category   "BARRR"
                                                              :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance ::venues.after-update {:id         2
                                                              :name       "Ho's Tavern"
                                                              :category   "BARRR"
                                                              :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   @*venues-awaiting-moderation*))))))))

;;; TODO -- should `after-update` automatically do things in a transaction? So if `after-update` fails, the original
;;; updates were canceled?
