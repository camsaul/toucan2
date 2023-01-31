(ns toucan2.tools.after-update-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.after-update :as after-update]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(derive ::venues.after-update ::test.track-realized/venues)

(defn- ensure-persistent! [x]
  (if (instance? clojure.lang.ITransientCollection x)
    (persistent! x)
    x))

(after-update/define-after-update ::venues.after-update
  [venue]
  (assert (map? venue))
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj (ensure-persistent! (select-keys venue [:id :name :category]))))
  nil)

(deftest ^:synchronized after-update-test
  (doseq [f [#'update/update!
             #_#'update/update-returning-pks!]] ; NOCOMMIT
    (testing f
      (test/with-discarded-table-changes :venues
        (binding [*venues-awaiting-moderation* (atom [])]
          (test.track-realized/with-realized-columns [realized-columns]
            (is (= (condp = f
                     #'update/update!               2
                     #'update/update-returning-pks! [1 2])
                   (f ::venues.after-update :category "bar" {:category "BARRR"})))
            (is (= #{:venues/id :venues/name :venues/category}
                   (realized-columns))))
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
    (swap! *recently-updated-venues* conj (ensure-persistent! (select-keys venue [:id :name]))))
  venue)

(deftest ^:synchronized compose-test
  (testing "after-update should compose"
    (test/with-discarded-table-changes :venues
      (binding [*venues-awaiting-moderation* (atom [])
                *recently-updated-venues*    (atom [])]
        (test.track-realized/with-realized-columns [realized-columns]
          (is (= 2
                 (update/update! ::venues.after-update.composed :category "bar" {:category "BARRR"})))
          (is (= #{:venues/name :venues/id :venues/category}
                 (realized-columns))))
        (testing (str '*venues-awaiting-moderation* " from " ::venues.after-update)
          (is (= [(instance/instance ::venues.after-update.composed {:id 1, :name "Tempest", :category "BARRR"})
                  (instance/instance ::venues.after-update.composed {:id 2, :name "Ho's Tavern", :category "BARRR"})]
                 @*venues-awaiting-moderation*)))
        (testing (str '*recently-updated-venues* " from " ::venues.after-update.composed)
          (is (= [(instance/instance ::venues.after-update.composed {:id 1, :name "Tempest"})
                  (instance/instance ::venues.after-update.composed {:id 2, :name "Ho's Tavern"})]
                 @*recently-updated-venues*)))))))

(derive ::people.record-updates ::test/people)

(def ^:private ^:dynamic *updated-people*)

(after-update/define-after-update ::people.record-updates
  [person]
  (when *updated-people*
    (swap! *updated-people* conj (:id person)))
  person)

(deftest ^:synchronized only-call-once-test
  (test/with-discarded-table-changes :people
    (testing "after-update method should be applied exactly once"
      (binding [*updated-people* (atom [])]
        (is (= 1
               (update/update! ::people.record-updates 1 {:name "CAM"})))
        (is (= [1]
               @*updated-people*))
        (is (= {:id 1, :name "CAM"}
               (select/select-one [::people.record-updates :id :name] 1)))))))

(derive ::venues.exception.clojure-land ::test/venues)

(after-update/define-after-update ::venues.exception.clojure-land
  [venue]
  (update/update! ::test/venues 1 {:category "place"})
  ;; trigger a Clojure-land error
  (when (= (:category venue) "store")
    (throw (ex-info "Don't update a store!" {:venue venue})))
  venue)

(derive ::venues.exception.db-land ::test/venues)

(after-update/define-after-update ::venues.exception.db-land
  [venue]
  (update/update! ::test/venues 1 {:category "place"})
  ;; trigger a DB-land error
  (when (= (:category venue) "store")
    (update/update! ::test/venues 1 {:venue_name "this column doesn't exist"}))
  venue)

(deftest ^:synchronized exception-test
  (doseq [model [::venues.exception.clojure-land
                 ::venues.exception.db-land]]
    (testing (format "Model = %s" model)
      (testing "\nexception in after-update"
        (test/with-discarded-table-changes :venues
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               (case model
                 ::venues.exception.clojure-land #"Don't update a store"
                 ::venues.exception.db-land      (case (test/current-db-type)
                                                   :postgres #"ERROR: column \"venue_name\" of relation \"venues\" does not exist"
                                                   :h2       #"Column \"VENUE_NAME\" not found"
                                                   :mariadb  #"FIXME"))
               (update/update! model 2 {:category "store", :name "My Store"})))
          (testing "\nShould be done inside a transaction"
            (is (= [(instance/instance model
                                       {:id         1
                                        :name       "Tempest"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance model
                                       {:id         2
                                        :name       "Ho's Tavern"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                    (instance/instance model
                                       {:id         3
                                        :name       "BevMo"
                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                   (select/select [model :id :name :updated-at]
                                  {:order-by [[:id :asc]]})))))))))
