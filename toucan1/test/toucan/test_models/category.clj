(ns toucan.test-models.category
  "A model with custom implementations of:

   *  `types`                       -- `:lowercase-string` for `:name`, which lowercases values coming in or out of
                                       the DB;
   *  `pre-update` and `pre-insert` -- which check that a parent Category exists when setting `:parent-category-id`;
   *  `post-insert`                 -- which adds IDs of newly created Categories to a \"moderation queue\";
   *  `pre-delete`                  -- which deletes child Categories when deleting a Category;"
  (:require
   [clojure.string :as s]
   [methodical.core :as m]
   [toucan.db :as db]
   [toucan.models :as models]
   [toucan2.test :as test]
   [toucan2.tools.after-update :as after-update]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.helpers :as helpers]))

(models/defmodel Category :t1_categories)

(defn- maybe-lowercase-string [s]
  (when s
    (s/lower-case s)))

;; define a new custom type that will automatically lowercase strings coming into or out of the DB
(def lowercase-string-xform
  {:in  maybe-lowercase-string
   :out maybe-lowercase-string})

(helpers/deftransforms Category
  {:name lowercase-string-xform})

(defn- assert-parent-category-exists [{:keys [parent-category-id], :as category}]
  (when parent-category-id
    (binding [toucan2.util/*debug* true]
      (println (list 'db/exists? Category :id parent-category-id) '=> (db/exists? Category :id parent-category-id))) ; NOCOMMIT
    (assert (db/exists? Category :id parent-category-id)
            (format "A category with ID %d does not exist." parent-category-id)))
  category)

(defn- delete-child-categories! [{:keys [id]}]
  (db/delete! Category :parent-category-id id))

(def categories-awaiting-moderation
  "A poor man's message queue of newly added Category IDs that are \"awating moderation\"."
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn add-category-to-moderation-queue! [{:keys [id], :as new-category}]
  (swap! categories-awaiting-moderation conj id)
  new-category)

(def categories-recently-updated
  "A simple queue of recently updated Category IDs."
  (atom (clojure.lang.PersistentQueue/EMPTY)))

(defn add-category-to-updated-queue! [{:keys [id]}]
  (swap! categories-recently-updated conj id))

(helpers/define-before-insert Category
  [category]
  (assert-parent-category-exists category)
  category)

(helpers/define-after-insert Category
  [category]
  (add-category-to-moderation-queue! category)
  category)

(before-update/define-before-update Category
  [category]
  (assert-parent-category-exists category))

(after-update/define-after-update Category
  [category]
  (add-category-to-updated-queue! category))

(helpers/define-before-delete Category
  [category]
  (delete-child-categories! category))


(m/defmethod test/create-table-sql-file Category
  [_table-name]
  "toucan1/test/toucan/test_models/category.sql")
