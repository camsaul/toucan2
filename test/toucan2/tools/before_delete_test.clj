(ns toucan2.tools.before-delete-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [clojure.walk :as walk]
   [toucan2.delete :as delete]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.before-delete :as before-delete]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues.before-delete ::test/venues)

(def ^:dynamic ^:private *deleted-venues* nil)

(before-delete/define-before-delete ::venues.before-delete
  [venue]
  (when *deleted-venues*
    (swap! *deleted-venues* conj (dissoc venue :created-at :updated-at)))
  nil)

(deftest ^:synchronized before-delete-test
  (test/with-discarded-table-changes :venues
    (binding [*deleted-venues* (atom [])]
      (execute/with-call-count [call-count]
        (is (= 2
               (delete/delete! ::venues.before-delete :category "bar")))
        (testing "call count"
          (is (= 2 (call-count)))))
      (is (= [(instance/instance ::venues.before-delete
                                 {:id 3, :name "BevMo", :category "store"})]
             (select/select [::venues.before-delete :id :name :category])))
      (is (= [(instance/instance ::venues.before-delete
                                 {:id         1
                                  :name       "Tempest"
                                  :category   "bar"})
              (instance/instance ::venues.before-delete
                                 {:id         2
                                  :name       "Ho's Tavern"
                                  :category   "bar"})]
             @*deleted-venues*)))))

(derive ::venues.before-delete-exception.clojure-land ::test/venues)

(def ^:dynamic ^:private *before-delete-calls* nil)

(before-delete/define-before-delete ::venues.before-delete-exception.clojure-land
  [venue]
  (when *before-delete-calls*
    (swap! *before-delete-calls* conj [::venues.before-delete-exception.clojure-land (:id venue)]))
  (update/update! ::test/venues (:id venue) {:updated-at (LocalDateTime/parse "2022-08-16T14:22:00")})
  (when (= (:category venue) "store")
    (throw (ex-info "Don't delete a store!" {:venue venue}))))

(derive ::venues.before-delete-exception.db-land ::test/venues)

(before-delete/define-before-delete ::venues.before-delete-exception.db-land
  [venue]
  (when *before-delete-calls*
    (swap! *before-delete-calls* conj [::venues.before-delete-exception.db-land (:id venue)]))
  (when (= (:id venue) 2)
    (delete/delete! ::test/venues 2))
  (when (= (:id venue) 3)
    (update/update! ::test/venues 3 {:id 1})))

(deftest ^:synchronized before-delete-exception-test
  (doseq [model [::venues.before-delete-exception.clojure-land
                 ::venues.before-delete-exception.db-land]]
    (testing (format "Model = %s" model)
      (binding [*before-delete-calls* (atom [])]
        (testing "\nexception in before-delete"
          (test/with-discarded-table-changes :venues
            (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo
                 (case model
                   ::venues.before-delete-exception.clojure-land #"Don't delete a store"
                   ::venues.before-delete-exception.db-land      (case (test/current-db-type)
                                                                   :postgres #"ERROR: duplicate key value violates unique constraint"
                                                                   :h2       #"Unique index or primary key violation"
                                                                   :mariadb  #"Duplicate entry"))
                 (delete/delete! model)))
            (is (= [[model 1]
                    [model 2]
                    [model 3]]
                   @*before-delete-calls*))
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
                                    {:order-by [[:id :asc]]}))))))))))

(derive ::venues.before-delete.composed ::venues.before-delete)

(def ^:dynamic ^:private *deleted-venues-2* nil)

(before-delete/define-before-delete ::venues.before-delete.composed
  [venue]
  (when *deleted-venues-2*
    (swap! *deleted-venues-2* conj (dissoc venue :created-at :updated-at)))
  nil)

(deftest ^:synchronized compose-test
  (test/with-discarded-table-changes :venues
    (binding [*deleted-venues*   (atom [])
              *deleted-venues-2* (atom [])]
      (execute/with-call-count [call-count]
        (is (= 2
               (delete/delete! ::venues.before-delete.composed :category "bar")))
        (is (= 2
               (call-count))))
      (doseq [varr [#'*deleted-venues*
                    #'*deleted-venues-2*]]
        (testing varr
          (is (= [(instance/instance ::venues.before-delete.composed
                                     {:id       1
                                      :name     "Tempest"
                                      :category "bar"})
                  (instance/instance ::venues.before-delete.composed
                                     {:id       2
                                      :name     "Ho's Tavern"
                                      :category "bar"})]
                 @@varr)))))))

(derive ::venues.record-before-delete ::test/venues)

(before-delete/define-before-delete ::venues.record-before-delete
  [venue]
  (when *deleted-venues*
    (swap! *deleted-venues* conj (:id venue)))
  venue)

(deftest ^:synchronized only-call-once-test
  (testing "before-delete should only be called once"
    (test/with-discarded-table-changes :venues
      (binding [*deleted-venues* (atom [])]
        (is (= 1
               (delete/delete! ::venues.record-before-delete :id 1)))
        (is (= [1]
               @*deleted-venues*))))))

(derive ::venues.recursive-before-delete ::test/venues)

(before-delete/define-before-delete ::venues.recursive-before-delete
  [{venue-id :id, :as venue}]
  (when *deleted-venues*
    (swap! *deleted-venues* conj venue-id))
  ;; recursively delete the venue with the ID immediately before this one.
  (when (pos? venue-id)
    (delete/delete! ::venues.recursive-before-delete :id (dec venue-id)))
  venue)

(deftest ^:synchronized recursive-before-delete-test
  (testing "If before-delete recursively deletes something, it should trigger those object(s) before-delete methods"
    (test/with-discarded-table-changes :venues
      (binding [*deleted-venues* (atom [])]
        (testing "Only count the top-level delete for update-count purposes"
          (is (= 1
                 (delete/delete! ::venues.recursive-before-delete :id 3))))
        (is (= [3 2 1]
               @*deleted-venues*))
        (is (= nil
               (select/select-fn-set :id ::test/venues)))))))

(deftest ^:parallel macroexpansion-test
  (testing "define-before-delete should define vars with different names based on the model."
    (letfn [(generated-name* [form]
              (cond
                (sequential? form)
                (some generated-name* form)

                (and (symbol? form)
                     (str/starts-with? (name form) "before-delete"))
                form))
            (generated-name [form]
              (let [expanded (walk/macroexpand-all form)]
                (or (generated-name* expanded)
                    ['no-match expanded])))]
      (is (= 'before-delete-primary-method-model-1
             (generated-name `(before-delete/define-before-delete :model-1
                                [~'venue]
                                ~'venue))))
      (is (= 'before-delete-primary-method-model-2
             (generated-name `(before-delete/define-before-delete :model-2
                                [~'venue]
                                ~'venue)))))))
