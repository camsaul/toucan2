(ns toucan.models-test
  (:require
   [clojure.test :refer :all]
   [toucan.db :as t1.db]
   [toucan.models :as t1.models]
   [toucan.test-models.category :as category :refer [Category]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan.test-setup :as test-setup]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.test :as test])
  (:import
   (java.time LocalDateTime)))

(use-fixtures :each test-setup/do-with-default-quoting-style test/do-db-types-fixture)

(set! *warn-on-reflection* true)

(deftest resolve-model-test
  (are [x] (= :toucan.test-models.category/Category
              (t1.models/resolve-model x))
    Category
    'Category
    [Category]
    ['Category])
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Invalid model: 2"
       (t1.models/resolve-model 2)))
  (is (thrown-with-msg?
       clojure.lang.ExceptionInfo
       #"Invalid model: :some-other-keyword"
       (t1.models/resolve-model :some-other-keyword))))

(deftest types-test
  (testing ":bar should come back as a Keyword even though it's a VARCHAR in the DB, just like :name"
    (doseq [model [Venue
                   'Venue]
            model [model
                   [model :id :name :category]]]
      (testing (format "model = %s" (pr-str model)))
      (is (= {:category :bar, :name "Tempest", :id 1}
             (t1.db/select-one Venue :id 1)))))
  (testing "should still work when not fetching whole object"
    (is (= :bar
           (t1.db/select-one-field :category Venue :id 1))))
  (testing "should also work when setting values"
    (test/with-discarded-table-changes Venue
      (is (= true
             (t1.db/update! Venue 1 :category :dive-bar)))
      (is (= {:category :dive-bar, :name "Tempest", :id 1}
             (t1.db/select-one Venue :id 1)))))
  (testing "Make sure namespaced keywords are preserved"
    (test/with-discarded-table-changes Venue
      (is (= true
             (t1.db/update! Venue 1 :category :bar/dive-bar)))
      (is (= {:category :bar/dive-bar, :name "Tempest", :id 1}
             (t1.db/select-one Venue :id 1))))))

(deftest custom-types-test
  (testing (str "Test custom types. Category.name is a custom type, :lowercase-string, that automatically "
                "lowercases strings as they come in")
    (test/with-discarded-table-changes Category
      (is (= {:id 5, :name "wine-bar", :parent-category-id nil}
             (t1.db/insert! Category :name "Wine-Bar"))))
    (test/with-discarded-table-changes Category
      (is (= true
             (t1.db/update! Category 1, :name "Bar-Or-Club")))
      (is (= {:id 1, :name "bar-or-club", :parent-category-id nil}
             (t1.db/select-one Category 1))))))

(defn- timestamp-after-jan-first? [^LocalDateTime t]
  (.isAfter t (LocalDateTime/parse "2017-01-01T00:00:00")))

(deftest insert!-properties-test
  (testing (str "calling insert! for Venue should trigger the :timestamped? :insert function which should set an "
                "appropriate :created-at and :updated-at value")
    (test/with-discarded-table-changes Venue
      (t1.db/insert! Venue, :name "Zeitgeist", :category "bar")
      (is (= {:created-at true, :updated-at true}
             (-> (t1.db/select-one [Venue :created-at :updated-at] :name "Zeitgeist")
                 (update :created-at timestamp-after-jan-first?)
                 (update :updated-at timestamp-after-jan-first?)))))))

(deftest update!-properties-test
  (testing (str "calling update! for Venue should trigger the :timestamped? :insert function which in this case "
                "updates :updated-at (but not :created-at)")
    (test/with-discarded-table-changes Venue
      (let [venue (t1.db/insert! Venue, :name "Zeitgeist", :category "bar")]
        (Thread/sleep 1000)
        (is (= true
               (t1.db/update! Venue (:id venue) :category "dive-bar"))))
      (let [{:keys [created-at updated-at]} (t1.db/select-one [Venue :created-at :updated-at] :name "Zeitgeist")]
        (is (.isAfter ^LocalDateTime updated-at ^LocalDateTime created-at))))))

;; for Category, we set up `pre-insert` and `pre-update` to assert that a Category with `parent-category-id` exists
;; before setting it.

(deftest pre-insert-test
  (is (thrown-with-msg?
       Exception
       #"A category with ID 100 does not exist"
       (test/with-discarded-table-changes Category
         (t1.db/insert! Category :name "seafood", :parent-category-id 100))))
  (test/with-discarded-table-changes Category
    (testing "Sanity check: Category 1 should exist"
      (is (t1.db/exists? Category :id 1)))
    (is (= {:id 5, :name "seafood", :parent-category-id 1}
           (t1.db/insert! Category :name "seafood", :parent-category-id 1)))))

(deftest pre-update-test
  (test/with-discarded-table-changes Category
    (is (thrown-with-msg?
         Exception
         #"A category with ID 100 does not exist"
         (t1.db/update! Category 2 :parent-category-id 100))))
  (test/with-discarded-table-changes Category
    (is (= true
           (t1.db/update! Category 2 :parent-category-id 4)))))

;; Categories adds the IDs of recently created Categories to a "moderation queue" as part of its `post-insert`
;; implementation; check that creating a new Category results in the ID of the new Category being at the front of the
;; queue
(deftest post-insert-test
  (test/with-discarded-table-changes Category
    (reset! category/categories-awaiting-moderation (clojure.lang.PersistentQueue/EMPTY))
    (t1.db/insert! Category :name "toucannery")
    (is (= 5
           (peek @category/categories-awaiting-moderation)))))

;; Categories adds the IDs of recently updated Categories to a "update queue" as part of its `post-update`
;; implementation; check that updating a Category results in the ID of the updated Category being at the front of the
;; queue
(deftest post-update-test
  (test/with-discarded-table-changes Category
    (reset! category/categories-recently-updated (clojure.lang.PersistentQueue/EMPTY))
    (is (= true
           (t1.db/update! Category 2 :name "lobster")))
    (is (= 2
           (peek @category/categories-recently-updated))))
  (test/with-discarded-table-changes Category
    (reset! category/categories-recently-updated (clojure.lang.PersistentQueue/EMPTY))
    (is (= true
           (t1.db/update! Category 1 :name "fine-dining")))
    (is (= true
           (t1.db/update! Category 2 :name "steak-house")))
    (is (= [1 2]
           @category/categories-recently-updated))))

;; For Category, deleting a parent category should also delete any child categories.
(deftest pre-delete-test
  (test/with-discarded-table-changes Category
    (is (= true
           (t1.db/delete! Category :id 1)))
    (is (= #{{:id 3, :name "resturaunt", :parent-category-id nil}
             {:id 4, :name "mexican-resturaunt", :parent-category-id 3}}
           (set (t1.db/select Category)))))
  (testing "shouldn't delete anything else if the Category is not parent of anybody else"
    (test/with-discarded-table-changes Category
      (is (= true
             (t1.db/delete! Category :id 2)))
      (is (= #{{:id 1, :name "bar", :parent-category-id nil}
               {:id 3, :name "resturaunt", :parent-category-id nil}
               {:id 4, :name "mexican-resturaunt", :parent-category-id 3}}
             (set (t1.db/select Category)))))))

(deftest default-fields-test
  (testing "check that we can still override default-fields"
    (is (= {:created-at (LocalDateTime/parse "2017-01-01T00:00:00")}
           (t1.db/select-one [Venue :created-at] :id 1)))))

(deftest model-in-honeysql-test
  (testing "Test using model in HoneySQL form"
    (is (= [{:id 1, :name "Tempest"}
            {:id 2, :name "Ho's Tavern"}
            {:id 3, :name "BevMo"}]
           (binding [conn/*current-connectable* ::test-setup/db]
             (t1.db/query {:select   [:id :name]
                        :from     [(keyword (model/table-name Venue))]
                        :order-by [:id]}))))))

(deftest empty-test
  (testing "Test (empty)"
    (is (= {}
           (empty (t1.db/select-one Venue :name "BevMo"))))))

(deftest map->-test
  (testing `t1.models/map->
    (is (instance/instance-of? Venue (t1.models/map-> Venue {:name "BevMo"})))
    (is (= (instance/instance Venue {:name "BevMo"})
           (t1.models/map-> Venue {:name "BevMo"})))))
