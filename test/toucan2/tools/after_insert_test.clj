(ns toucan2.tools.after-insert-test
  (:require
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.test :as test]
   [toucan2.tools.after-insert :as after-insert])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues.after-insert ::test/venues)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(after-insert/define-after-insert ::venues.after-insert
  [venue]
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj venue))
  (assoc venue :awaiting-moderation? true))

(deftest after-insert-test
  (doseq [f [#'insert/insert!
             #'insert/insert-returning-pks!
             #'insert/insert-returning-instances!]]
    (testing f
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (is (= (condp = f
                   #'insert/insert! 1
                   #'insert/insert-returning-pks! [4]
                   #'insert/insert-returning-instances! [{:id                   4
                                                          :name                 "Lombard Heights Market"
                                                          :category             "liquor-store"
                                                          :created-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                          :updated-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                          :awaiting-moderation? true}])
                 (f ::venues.after-insert {:name "Lombard Heights Market", :category "liquor-store"})))
          (testing "should be added to *venues-awaiting-moderation*"
            (is (= [(instance/instance
                     ::venues.after-insert
                     {:id         4
                      :name       "Lombard Heights Market"
                      :category   "liquor-store"
                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   @*venues-awaiting-moderation*))))))))
