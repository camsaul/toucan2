(ns toucan2.tools.after-insert-test
  (:require
   [clojure.test :refer :all]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-insert :as after-insert]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.after-update :as after-update]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues.after-insert ::test/venues)

(def ^:private ^:dynamic *venues-awaiting-moderation* nil)

(after-insert/define-after-insert ::venues.after-insert
  [venue]
  ;; make sure this is treated as a REAL function tail.
  {:pre [(map? venue)], :post [(:awaiting-moderation? %)]}
  (testing (format "venue = %s" (pr-str venue))
    (is (isa? (protocols/model venue) ::test/venues))
    #_(is (instance/instance-of? ::test/venues venue)))
  (when *venues-awaiting-moderation*
    (swap! *venues-awaiting-moderation* conj (realize/realize venue)))
  (assoc venue :awaiting-moderation? true))

(derive ::venues.after-insert.composed ::venues.after-insert)

(after-insert/define-after-insert ::venues.after-insert.composed
  [venue]
  (assoc venue :composed? true))

(deftest ^:synchronized after-insert-test
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

(derive ::venues.after-insert.after-select ::venues.after-insert)

(after-select/define-after-select ::venues.after-insert.after-select
  [venue]
  (assoc venue :after-select? true))

(deftest ^:synchronized do-after-select-test
  (testing "insert-returning-instances! should do after-select as well"
    ;; TODO -- what about insert that doesn't return instances? I guess it has to be 'upgraded' to return instances in
    ;; order to do after-insert stuff...
    (test/with-discarded-table-changes :venues
      (binding [*venues-awaiting-moderation* (atom [])]
        (is (= [{:id                   4
                 :name                 "Lombard Heights Market"
                 :category             "liquor-store"
                 :created-at           (LocalDateTime/parse "2017-01-01T00:00")
                 :updated-at           (LocalDateTime/parse "2017-01-01T00:00")
                 :awaiting-moderation? true
                 :after-select?        true}]
               (insert/insert-returning-instances! ::venues.after-insert.after-select
                                                   {:name "Lombard Heights Market", :category "liquor-store"})))
        ;; I guess after-insert happens before after-select? Not 100% sure whether this makes sense.
        (testing "should be added to *venues-awaiting-moderation*"
          (is (= [(instance/instance
                   ::venues.after-insert.after-select
                   {:id         4
                    :name       "Lombard Heights Market"
                    :category   "liquor-store"
                    :created-at (LocalDateTime/parse "2017-01-01T00:00")
                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                 @*venues-awaiting-moderation*)))))))

(derive ::people.record-inserts ::test/people)

(def ^:private ^:dynamic *inserted-people* nil)

(after-insert/define-after-insert ::people.record-inserts
  [person]
  (when *inserted-people*
    (swap! *inserted-people* conj (:name person)))
  person)

(deftest ^:synchronized only-call-once-test
  (test/with-discarded-table-changes :people
    (testing "after-insert method should be applied exactly once"
      (binding [*inserted-people* (atom [])]
        (is (= [5]
               (insert/insert-returning-pks! ::people.record-inserts {:name "CAM"})))
        (is (= ["CAM"]
               @*inserted-people*))
        (is (= {:name "CAM"}
               (select/select-one [::people.record-inserts :name] 5)))))))

(derive ::venues.exception.clojure-land ::test/venues)

(after-insert/define-after-insert ::venues.exception.clojure-land
  [venue]
  (insert/insert! ::test/venues {:name "ANOTHER STORE", :category "bar"})
  ;; trigger a Clojure-land error
  (when (= (:category venue) "store")
    (throw (ex-info "Don't insert a store!" {:venue venue})))
  venue)

(derive ::venues.exception.db-land ::test/venues)

(after-insert/define-after-insert ::venues.exception.db-land
  [venue]
  (insert/insert! ::test/venues {:name "ANOTHER STORE", :category "bar"})
  ;; trigger a DB-land error
  (when (= (:category venue) "store")
    (insert/insert! ::test/venues {:name "STORE 1", :category "bar", :id 1}))
  venue)

(deftest ^:synchronized exception-test
  (doseq [model [::venues.exception.clojure-land
                 ::venues.exception.db-land]]
    (testing (format "Model = %s" model)
      (testing "\nexception in after-insert"
        (test/with-discarded-table-changes :venues
          (is (thrown-with-msg?
               clojure.lang.ExceptionInfo
               (case model
                 ::venues.exception.clojure-land #"Don't insert a store"
                 ::venues.exception.db-land      (case (test/current-db-type)
                                                   :postgres #"ERROR: duplicate key value violates unique constraint"
                                                   :h2       #"Unique index or primary key violation"))
               (insert/insert! model {:category "store", :name "My Store"})))
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

(derive ::people.add-favorite-bird-type ::test/people)

(after-insert/define-after-insert ::people.add-favorite-bird-type
  [person]
  (assoc person :favorite-bird-type "toucan"))

(deftest ^:synchronized reset-changes-test
  (test/with-discarded-table-changes :people
    (testing "Changes made inside after-insert should not be considered part of the instance changes"
      (let [[row] (insert/insert-returning-instances! ::people.add-favorite-bird-type {:name "Cam Era"})]
        (is (= {:id                 5
                :name               "Cam Era"
                :created-at         nil
                :favorite-bird-type "toucan"}
               row))
        (testing `protocols/original
          (is (= {:id                 5
                  :name               "Cam Era"
                  :created-at         nil
                  :favorite-bird-type "toucan"}
                 (protocols/original row))))
        (testing `protocols/changes
          (is (= nil
                 (protocols/changes row))))))))

;;; the [[update-test]] bug below was only triggering if at least one after-update method was defined.

(derive ::venues.after-update ::test/venues)

(after-update/define-after-update ::venues.after-update
  [venue]
  venue)

(deftest ^:synchronized update-test
  (testing "You should be able to do update! if you have an after-insert method defined, but no after-update defined"
    (doseq [f [#'update/update!
               #'update/update-returning-pks!]]
      (testing f
        (test/with-discarded-table-changes :venues
          (is (some? (f ::venues.after-insert 1 {:name "Lombard Heights Market", :category "liquor-store"})))
          (is (= {:name "Lombard Heights Market"}
                 (select/select-one [::venues.after-insert :name] 1))))))))
