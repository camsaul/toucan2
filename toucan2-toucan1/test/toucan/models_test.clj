(ns toucan.models-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [toucan.db :as db]
            [toucan.test-models.category :as category :refer [Category]]
            [toucan.test-models.venue :refer [Venue]]
            [toucan.test-setup :as test]))

(use-fixtures :once test/do-with-reset-db)
(use-fixtures :each test/do-with-default-connection)

(deftest types-test
  (testing ":bar should come back as a Keyword even though it's a VARCHAR in the DB, just like :name"
    (is (= {:category :bar, :name "Tempest", :id 1}
           (db/select-one Venue :id 1))))
  (testing "should still work when not fetching whole object"
    (is (= :bar
           (db/select-one-field :category Venue :id 1))))
  (testing "should also work when setting values"
    (test/with-clean-db
      (is (= true
             (db/update! Venue 1 :category :dive-bar)))
      (is (= {:category :dive-bar, :name "Tempest", :id 1}
             (db/select-one Venue :id 1)))))
  (testing "Make sure namespaced keywords are preserved"
    (test/with-clean-db
      (is (= true
             (db/update! Venue 1 :category :bar/dive-bar)))
      (is (= {:category :bar/dive-bar, :name "Tempest", :id 1}
             (db/select-one Venue :id 1))))))

(deftest custom-types-test
  (testing (str "Test custom types. Category.name is a custom type, :lowercase-string, that automatically "
                "lowercases strings as they come in")
    (test/with-clean-db
      (is (= {:id 5, :name "wine-bar", :parent-category-id nil}
             (db/insert! Category, :name "Wine-Bar"))))
    (test/with-clean-db
      (is (= true
             (db/update! Category 1, :name "Bar-Or-Club")))
      (is (= {:id 1, :name "bar-or-club", :parent-category-id nil}
             (Category 1))))))

(defn- timestamp-after-jan-first? [t]
  (t/after? t (t/local-date-time "2017-01-01T00:00:00")))

(deftest insert!-properties-test
  (testing (str "calling insert! for Venue should trigger the :timestamped? :insert function which should set an "
                "appropriate :created-at and :updated-at value")
    (test/with-clean-db
      (db/insert! Venue, :name "Zeitgeist", :category "bar")
      (is (= {:created-at true, :updated-at true}
             (-> (db/select-one [Venue :created-at :updated-at] :name "Zeitgeist")
                 (update :created-at timestamp-after-jan-first?)
                 (update :updated-at timestamp-after-jan-first?)))))))

(deftest update!-properties-test
  (testing (str "calling update! for Venue should trigger the :timestamped? :insert function which in this case "
                "updates :updated-at (but not :created-at)")
    (test/with-clean-db
      (let [venue (db/insert! Venue, :name "Zeitgeist", :category "bar")]
        (Thread/sleep 1000)
        (is (= true
               (db/update! Venue (:id venue) :category "dive-bar"))))
      (let [{:keys [created-at updated-at]} (db/select-one [Venue :created-at :updated-at] :name "Zeitgeist")]
        (is (t/after? updated-at created-at))))))

;; for Category, we set up `pre-insert` and `pre-update` to assert that a Category with `parent-category-id` exists
;; before setting it.

(deftest pre-insert-test
  (is (thrown-with-msg?
       Exception
       #"A category with ID 100 does not exist"
       (test/with-clean-db
         (db/insert! Category :name "seafood", :parent-category-id 100))))
  (test/with-clean-db
    (is (= {:id 5, :name "seafood", :parent-category-id 1}
           (db/insert! Category :name "seafood", :parent-category-id 1)))))

(deftest pre-update-test
  (test/with-clean-db
    (is (thrown-with-msg?
         Exception
         #"A category with ID 100 does not exist"
         (db/update! Category 2 :parent-category-id 100))))
  (test/with-clean-db
    (is (= true
           (db/update! Category 2 :parent-category-id 4)))))

;; Categories adds the IDs of recently created Categories to a "moderation queue" as part of its `post-insert`
;; implementation; check that creating a new Category results in the ID of the new Category being at the front of the
;; queue
(deftest post-insert-test
  (test/with-clean-db
    (reset! category/categories-awaiting-moderation (clojure.lang.PersistentQueue/EMPTY))
    (db/insert! Category :name "toucannery")
    (is (= 5
           (peek @category/categories-awaiting-moderation)))))

;; Categories adds the IDs of recently updated Categories to a "update queue" as part of its `post-update`
;; implementation; check that updating a Category results in the ID of the updated Category being at the front of the
;; queue
(deftest post-update-test
  (test/with-clean-db
    (reset! category/categories-recently-updated (clojure.lang.PersistentQueue/EMPTY))
    (is (= true
           (db/update! Category 2 :name "lobster")))
    (is (= 2
           (peek @category/categories-recently-updated))))
  (test/with-clean-db
    (reset! category/categories-recently-updated (clojure.lang.PersistentQueue/EMPTY))
    (is (= true
           (db/update! Category 1 :name "fine-dining")))
    (is (= true
           (db/update! Category 2 :name "steak-house")))
    (is (= [1 2]
           @category/categories-recently-updated))))

;; For Category, deleting a parent category should also delete any child categories.
(deftest pre-delete-test
  (test/with-clean-db
    (is (= true
           (db/delete! Category :id 1)))
    (is (= #{{:id 3, :name "resturaunt", :parent-category-id nil}
             {:id 4, :name "mexican-resturaunt", :parent-category-id 3}}
           (set (Category)))))
  (testing "shouldn't delete anything else if the Category is not parent of anybody else"
    (test/with-clean-db
      (is (= true
             (db/delete! Category :id 2)))
      (is (= #{{:id 1, :name "bar", :parent-category-id nil}
               {:id 3, :name "resturaunt", :parent-category-id nil}
               {:id 4, :name "mexican-resturaunt", :parent-category-id 3}}
             (set (Category)))))))

(deftest default-fields-test
  (testing "check that we can still override default-fields"
    (is (= {:created-at (t/local-date-time "2017-01-01T00:00:00")}
           (db/select-one [Venue :created-at] :id 1)))))

(deftest invoke-model-no-args-test
  (testing "Test invoking model as a function (no args)"
    (is (= [{:category :bar, :name "Tempest", :id 1}
            {:category :bar, :name "Ho's Tavern", :id 2}
            {:category :store, :name "BevMo", :id 3}]
           (sort-by :id (Venue))))))

(deftest invoke-model-one-arg-test
  (testing "Test invoking model as a function (single arg, id)"
    (is (= {:category :bar, :name "Tempest", :id 1}
           (Venue 1)))))

(deftest invoke-model-multiple-args-test
  (testing "Test invoking model as a function (kwargs)"
    (is (= {:category :store, :name "BevMo", :id 3}
           (Venue :name "BevMo")))))

(deftest invoke-model-apply-test
  (testing "Test invoking model as a function with apply"
    (is (= {:category :store, :name "BevMo", :id 3}
           (apply Venue [:name "BevMo"])))))

(deftest model-in-honeysql-test
  (testing "Test using model in HoneySQL form"
    (is (= [{:id 1, :name "Tempest"}
            {:id 2, :name "Ho's Tavern"}
            {:id 3, :name "BevMo"}]
           (db/query {:select   [:id :name]
                      :from     [Venue]
                      :order-by [:id]})))))

(deftest empty-test
  (testing "Test (empty)"
    (is (= {}
           (empty (Venue :name "BevMo"))))))
