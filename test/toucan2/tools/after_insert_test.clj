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

(use-fixtures :each test/do-db-types-fixture)

(derive ::venues.after-insert ::test/venues)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(after-insert/define-after-insert ::venues.after-insert
  [venue]
  ;; make sure this is treated as a REAL function tail.
  {:pre [(map? venue)], :post [(:awaiting-moderation? %)]}
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj venue))
  (assoc venue :awaiting-moderation? true))

(derive ::venues.after-insert.composed ::venues.after-insert)

(after-insert/define-after-insert ::venues.after-insert.composed
  [venue]
  (assoc venue :composed? true))

(deftest after-insert-test
  (doseq [f     [#'insert/insert!
                 #'insert/insert-returning-pks!
                 #'insert/insert-returning-instances!]
          model [::venues.after-insert
                 ::venues.after-insert.composed]]
    (testing (str f \newline model \newline)
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (is (= (condp = f
                   #'insert/insert!                     1
                   #'insert/insert-returning-pks!       [4]
                   #'insert/insert-returning-instances! [(merge
                                                          {:id                   4
                                                           :name                 "Lombard Heights Market"
                                                           :category             "liquor-store"
                                                           :created-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                           :updated-at           (LocalDateTime/parse "2017-01-01T00:00")
                                                           :awaiting-moderation? true}
                                                          (when (= model ::venues.after-insert.composed)
                                                            {:composed? true}))])
                 (f model {:name "Lombard Heights Market", :category "liquor-store"})))
          (testing "should be added to *venues-awaiting-moderation*"
            (is (= [(instance/instance
                     model
                     (merge
                      {:id         4
                       :name       "Lombard Heights Market"
                       :category   "liquor-store"
                       :created-at (LocalDateTime/parse "2017-01-01T00:00")
                       :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                      (when (= model ::venues.after-insert.composed)
                        {:composed? true})))]
                   @*venues-awaiting-moderation*))))))))
