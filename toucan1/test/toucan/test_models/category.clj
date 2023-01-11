(ns toucan.test-models.category
  "A model with custom implementations of:

   *  `types`                       -- `:lowercase-string` for `:name`, which lowercases values coming in or out of
                                       the DB;
   *  `pre-update` and `pre-insert` -- which check that a parent Category exists when setting `:parent-category-id`;
   *  `post-insert`                 -- which adds IDs of newly created Categories to a \"moderation queue\";
   *  `pre-delete`                  -- which deletes child Categories when deleting a Category;"
  (:require
   [methodical.core :as m]
   [toucan.db :as t1.db]
   [toucan.models :as t1.models]
   [toucan2.test :as test]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(t1.models/defmodel Category :t1_categories)

(defn- maybe-lowercase-string [s]
  (when s
    (u/lower-case-en s)))

;;; define a new custom type that will automatically lowercase strings coming into or out of the DB
(t1.models/add-type!
 :lowercase-string
 :in  maybe-lowercase-string
 :out maybe-lowercase-string)

(defn- assert-parent-category-exists [{:keys [parent-category-id], :as category}]
  (when parent-category-id
    (assert (t1.db/exists? Category :id parent-category-id)
            (format "A category with ID %d does not exist." parent-category-id)))
  category)

(defn- delete-child-categories! [{:keys [id]}]
  (t1.db/delete! Category :parent-category-id id))

(def ^:dynamic *categories-awaiting-moderation*
  "A poor man's message queue of newly added Category IDs that are \"awating moderation\"."
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn add-category-to-moderation-queue! [{:keys [id], :as new-category}]
  (swap! *categories-awaiting-moderation* conj id)
  new-category)

(def ^:dynamic *categories-recently-updated*
  "A simple queue of recently updated Category IDs."
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn add-category-to-updated-queue! [{:keys [id]}]
  (swap! *categories-recently-updated* conj id))

(t1.models/define-methods-with-IModel-method-map
 Category
 {:types       (constantly {:name :lowercase-string})
  :pre-insert  assert-parent-category-exists
  :post-insert add-category-to-moderation-queue!
  :pre-update  assert-parent-category-exists
  :post-update add-category-to-updated-queue!
  :pre-delete  delete-child-categories!})

(m/defmethod test/create-table-sql-file [:postgres Category]
  [_db-type _table-name]
  "toucan/test_models/category.postgres.sql")

(m/defmethod test/create-table-sql-file [:h2 Category]
  [_db-type _table-name]
  "toucan/test_models/category.h2.sql")
