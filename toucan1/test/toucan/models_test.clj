(ns toucan.models-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan.db :as t1.db]
   [toucan.models :as t1.models]
   [toucan.test-models.category :as category :refer [Category]]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan.test-setup :as test-setup]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.test :as test]
   [toucan2.tools.after-insert :as after-insert]
   [toucan2.tools.after-select :as after-select]
   [toucan2.tools.after-update :as after-update]
   [toucan2.tools.before-delete :as before-delete]
   [toucan2.tools.before-insert :as before-insert]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.hydrate :as hydrate]
   [toucan2.tools.transformed :as transformed])
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

(deftest properties-test
  (is (= {:toucan.test-models.venue/timestamped? true}
         (t1.models/properties Venue)))
  (is (= nil
         (t1.models/properties Category))))

(deftest primary-key-test
  (is (= :id
         (t1.models/primary-key Venue)))
  (is (= :number
         (t1.models/primary-key PhoneNumber))))

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

(deftest types-fn-test
  (testing `t1.models/types
    (is (= {:name :lowercase-string}
           (t1.models/types Category)))
    (are [direction] (= "wow"
                        ((get-in (transformed/transforms Category) [:name direction]) "WOW"))
      :in
      :out))
  (testing "Error on unknown types when invoking with a meaningful error message."
    (t1.models/deftypes ::TypesModel {:k ::fake-keyword})
    (is (thrown-with-msg?
         Exception
         #"Unregistered type: :toucan.models-test/fake-keyword. Known types:"
         ((get-in (transformed/transforms ::TypesModel) [:k :in]) "wow"))))
  (testing "Pick up changes to types"
    ;; make sure the type functions aren't registered yet.
    (remove-method @#'t1.models/type-fn [::to-number :in])
    (remove-method @#'t1.models/type-fn [::to-number :out])
    ;; should be able to dynamically resolve types defined AFTER the call to `deftypes`.
    (t1.models/deftypes ::TypesModel {:n ::to-number})
    (testing "type function is double"
      (t1.models/add-type! ::to-number {:in double, :out double})
      (let [f (get-in (transformed/transforms ::TypesModel) [:n :in])]
        (is (= 100.0
               (f 100)
               (f 100.0)))))
    (testing "type function is now long"
      (t1.models/add-type! ::to-number {:in long, :out long})
      (let [f (get-in (transformed/transforms ::TypesModel) [:n :in])]
        (is (= 100
               (f 100)
               (f 100.0)))))))

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

(deftest do-post-select-test
  (testing `t1.models/post-select
    ;; needs to pick up transforms AND `after-select`
    (after-select/define-after-select ::PostSelect
      [row]
      (assoc row :after-select? true))
    (transformed/deftransforms ::PostSelect
      {:name {:in str/upper-case, :out str/lower-case}})
    (testing `t1.models/post-select
      (is (= {:name "bevmo", :after-select? true}
             (t1.models/post-select (instance/instance ::PostSelect {:name "BevMo"})))))
    (testing `t1.models/do-post-select
      (is (= {:name "bevmo", :after-select? true}
             (t1.models/do-post-select ::PostSelect {:name "BevMo"}))))))

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

(deftest do-pre-insert-test
  (testing `t1.models/pre-insert
    ;; needs to pick up transforms AND `before-insert`
    (before-insert/define-before-insert ::BeforeInsert
      [row]
      (assoc row :before-insert? true))
    (transformed/deftransforms ::BeforeInsert
      {:name {:in str/upper-case, :out str/lower-case}})
    (testing `t1.models/do-pre-insert
      (is (= {:name "BEVMO", :before-insert? true}
             (t1.models/do-pre-insert ::BeforeInsert {:name "BevMo"}))))
    (testing `t1.models/pre-insert
      (is (= {:name "BEVMO", :before-insert? true}
             (t1.models/pre-insert (instance/instance ::BeforeInsert {:name "BevMo"})))))))

(deftest pre-update-test
  (test/with-discarded-table-changes Category
    (is (thrown-with-msg?
         Exception
         #"A category with ID 100 does not exist"
         (t1.db/update! Category 2 :parent-category-id 100))))
  (test/with-discarded-table-changes Category
    (is (= true
           (t1.db/update! Category 2 :parent-category-id 4)))))

(deftest do-pre-update-test
  ;; needs to pick up transforms AND `before-update`
  (before-update/define-before-update ::BeforeUpdate
    [row]
    (assoc row :before-update? true))
  (transformed/deftransforms ::BeforeUpdate
    {:name {:in str/upper-case, :out str/lower-case}})
  (testing `t1.models/do-pre-update
    (is (= {:name "BEVMO", :before-update? true}
           (t1.models/do-pre-update ::BeforeUpdate {:name "BevMo"}))))
  (testing `t1.models/pre-update
    (is (= {:name "BEVMO", :before-update? true}
           (t1.models/pre-update (instance/instance ::BeforeUpdate {:name "BevMo"}))))))

;; Categories adds the IDs of recently created Categories to a "moderation queue" as part of its `post-insert`
;; implementation; check that creating a new Category results in the ID of the new Category being at the front of the
;; queue
(deftest post-insert-test
  (test/with-discarded-table-changes Category
    (reset! category/categories-awaiting-moderation (clojure.lang.PersistentQueue/EMPTY))
    (t1.db/insert! Category :name "toucannery")
    (is (= 5
           (peek @category/categories-awaiting-moderation)))))

(deftest do-post-insert-test
  (testing `t1.models/post-insert
    ;; needs to pick up transforms AND `before-insert`
    (after-insert/define-after-insert ::PostInsert
      [row]
      (assoc row :after-insert? true))
    (transformed/deftransforms ::PostInsert
      {:name {:in str/upper-case, :out str/lower-case}})
    (testing `t1.models/post-insert
      (is (= {:name "bevmo", :after-insert? true}
             (t1.models/post-insert (instance/instance ::PostInsert {:name "BevMo"})))))))

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

(deftest do-post-update-test
  (testing `t1.models/post-update
    ;; needs to pick up transforms AND `after-update`
    (after-update/define-after-update ::PostUpdate
      [row]
      (assoc row :after-update? true))
    (transformed/deftransforms ::PostUpdate
      {:name {:in str/upper-case, :out str/lower-case}})
    (testing `t1.models/post-update
      (is (= {:name "bevmo", :after-update? true}
             (t1.models/post-update (instance/instance ::PostUpdate {:name "BevMo"})))))))

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

(deftest do-pre-delete-test
  (testing `t1.models/pre-delete
    ;; needs to pick up transforms AND `before-delete`
    (before-delete/define-before-delete ::BeforeDelete
      [row]
      (assoc row :before-delete? true))
    (transformed/deftransforms ::BeforeDelete
      {:name {:in str/upper-case, :out str/lower-case}})
    (testing `t1.models/pre-delete
      (is (= {:name "BEVMO", :before-delete? true}
             (t1.models/pre-delete (instance/instance ::BeforeDelete {:name "BevMo"})))))))

(deftest default-fields-test
  (testing "check that we can still override default-fields"
    (is (= {:created-at (LocalDateTime/parse "2017-01-01T00:00:00")}
           (t1.db/select-one [Venue :created-at] :id 1)))))

(deftest default-fields-fn-test
  (is (= [:id :name :category]
         (t1.models/default-fields Venue))))

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

(t1.models/define-hydration-keys ::FakeModel [::database ::db])

(deftest hydration-keys-test
  (is (= [::database ::db]
         (t1.models/hydration-keys ::FakeModel)))
  (are [k] (= ::FakeModel
              (hydrate/model-for-automagic-hydration :default k))
    ::database
    ::db))
