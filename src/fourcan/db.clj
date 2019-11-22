(ns fourcan.db
  (:require [fourcan
             [types :as types]
             [hierarchy :as hierarchy]]
            [fourcan.db.jdbc :as jdbc]
            [methodical.core :as m]
            [honeysql.helpers :as h]
            [clojure.string :as str]

            [fourcan.compile :as compile]))

(m/defmulti default-row-xform
  {:arglist '([model column-names rows])}
  types/dispatch-on-model
  :hierarchy #'hierarchy/hierarchy)

;; TODO - this is D U M B we shouldn't calculate the column names again for each row
(m/defmethod default-row-xform :default
  [_ cols row]
  (let [column-names (for [[_ column-name _] cols]
                       (keyword column-name))]
    (zipmap column-names row)))

(m/defmethod types/invoke-query :default
  [query]
  (jdbc/query (types/model query) (types/honeysql-form query) (merge {:row-xform (partial default-row-xform (types/model query))}
                                                                     (types/query-options query))))

(defn- select-pk [query pk-values]
  (h/merge-where query (compile/primary-key-where-clause query pk-values)))

(defn select
  {:arglists '([model-or-object pk-value-or-honeysql-form? & options])}
  ([x]
   (cond
     (types/query? x)    (types/query x x)
     (types/instance? x) (select-pk (select (types/model x)) (compile/primary-key-value x))
     (sequential? x)     (assoc (types/query (first x)) :fields (rest x))
     :else               (types/query x {:select [:*], :from [(types/model x)]})))

  ([model x]
   (if (map? x)
     (merge (select model) x)
     (select-pk (select model) x)))

  ([model k v & more]
   ;; TODO - have `compile-select-options` return where clause, not map
   (h/merge-where (types/query model) (:where (compile/compile-select-options (into [k v] more))))
   ;; TODO - Compile the optional where args
   ))

;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Imports from TOUCAN                                               |
;;; +----------------------------------------------------------------------------------------------------------------+

(defn select-one
  "Select a single object from the database.

     (select-one ['Database :name] :id 1) -> {:name \"Sample Dataset\"}"
  {:style/indent 1, :arglists '([object] [model & args])}
  [& args]
  (first (apply select (concat args [{:fields [:id :name], :limit 1, :order-by [[:id :asc]]}]))))

#_(defn select-one-field
  "Select a single `field` of a single object from the database.

     (select-one-field :name 'Database :id 1) -> \"Sample Dataset\""
  {:style/indent 2, :arglists '([field object] [field model & args])}
  [field model & args]
  {:pre [(keyword? field)]}
  (get (apply select-one [model field] args) field))

#_(defn select-one-id
  "Select the `:id` of a single object from the database.

     (select-one-id 'Database :name \"Sample Dataset\") -> 1"
  {:style/indent 1, :arglists '([object] [model & args])}
  [model & args]
  (let [pk-key (compile/primary-key model)]
    (if-not (sequential? pk-key)
      (apply select-one-field pk-key model args)
      (let [[result] (apply select-one (into [model] pk-key) args)]
        (when result
          (map result pk-key))))))

#_(defn select-field-seq
  {:style/indent 2, :arglists '([field object] [field model & args])}
  [field model & options]
  {:pre [(keyword? field)]}
  (seq (map field (apply select [model field] options))))

#_(defn select-field-set
  "Select values of a single field for multiple objects. These are returned as a set if any matching fields
   were returned, otherwise `nil`.

     (select-field :name 'Database) -> #{\"Sample Dataset\", \"test-data\"}"
  {:style/indent 2, :arglists '([field object] [field model & args])}
  [field model & args]
  (set (apply select-field-seq field model args)))

#_(defn select-id-seq
  [model & options]
  (let [pk-key (compile/primary-key model)]
    (if-not (sequential? pk-key)
      (apply select-field-seq pk-key model options)
      (when-let [results (seq (apply select (into [model] pk-key) options))]
        (for [result results]
          (mapv result pk-key))))))

#_(defn select-id-set
  "Select IDs for multiple objects. These are returned as a set if any matching IDs were returned, otherwise `nil`.

     (select-ids 'Table :db_id 1) -> #{1 2 3 4}"
  {:style/indent 1}
  [model & options]
  (set (apply select-id-seq model options)))

#_(defn select-field->field
  "Select fields `k` and `v` from objects in the database, and return them as a map from `k` to `v`.

     (select-field->field :id :name 'Database) -> {1 \"Sample Dataset\", 2 \"test-data\"}"
  {:style/indent 3}
  [k v model & options]
  {:pre [(keyword? k) (keyword? v)]}
  (into {} (for [result (apply select [model k v] options)]
             [(k result) (v result)])))

#_(defn select-field->id
  "Select `field` and `:id` from objects in the database, and return them as a map from `field` to `:id`.

     (select-field->id :name 'Database) -> {\"Sample Dataset\" 1, \"test-data\" 2}"
  {:style/indent 2}
  [field model & options]
  (let [pk-key (compile/primary-key model)]
    (if-not (sequential? pk-key)
      (apply select-field->field field pk-key model options)
      (when-let [rows (seq (apply select (into [model field] pk-key) options))]
        (into {} (for [row rows]
                   [(get row field) (mapv row pk-key)]))))))

#_(defn select-id->field
  "Select `field` and `:id` from objects in the database, and return them as a map from `:id` to `field`.

     (select-id->field :name 'Database) -> {1 \"Sample Dataset\", 2 \"test-data\"}"
  {:style/indent 2}
  [field model & options]
  (let [pk-key (compile/primary-key model)]
    (if-not (sequential? pk-key)
      (apply select-field->field pk-key field model options)
      (when-let [rows (seq (apply select (into [model field] pk-key) options))]
        (into {} (for [row rows]
                   [(mapv row pk-key) (get row field)]))))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                              Other query util fns                                              |
;;; +----------------------------------------------------------------------------------------------------------------+

#_(defn count
  "Select the count of objects matching some condition.

     ;; Get all Users whose email is non-nil
     (count 'User :email [:not= nil]) -> 12"
  {:style/indent 1}
  [model & options]
  (:count (apply select-one [model [:%count.* :count]] options)))

#_(defn exists?
  "Easy way to see if something exists in the DB.

    (db/exists? User :id 100)"
  {:style/indent 1}
  ([object]
   (exists? object {:where (compile/primary-key-where-clause object)}))

  ([model & options]
   (let [results (ops/query model (merge
                                   (compile/compile-select-options options)
                                   {:select [[true :exists]]
                                    :from   [(models/table model)]
                                    :limit  1}))]
     (boolean (seq results)))))


;;;                                                      update!
;;; ==================================================================================================================

#_(defn update!
  "Update a single row in the database. Returns updated object if a row was affected, `nil` otherwise. Accepts either an
  object, a single map of changes, or an id and keyword args. Various `pre-update` methods for the model are called
  before performing the updates, and `post-update` methods are called on the results.

     (db/update! (assoc (db/select-one Label 11) :name \"ToucanFriendly\"))
     (db/update! Label 11 :name \"ToucanFriendly\")
     (db/update! Label 11 {:name \"ToucanFriendly\"})"
  ([instance-or-instances]
   (if (sequential? instance-or-instances)
     (update! (first instance-or-instances) instance-or-instances)
     (update! instance-or-instances instance-or-instances)))

  ([model instance-or-instances]
   (when (seq instance-or-instances)
     (if (sequential? instance-or-instances)
       (seq (ops/update! model instance-or-instances))
       (first (ops/update! model [instance-or-instances])))))

  ([model pk-value changes]
   (when (seq changes)
     (update! model (instance/toucan-instance
                     model
                     (compile/assoc-primary-key  (instance/of model) pk-value)
                     (compile/dissoc-primary-key (instance/of model changes))
                     nil))))

  ([model pk-value k v & more]
   (update! model pk-value (u/varargs->map k v more))))

#_(defn update-where!
  "Convenience for updating several objects matching `conditions-map`. Selects objects matching `conditions` map, then
  calls `update!` on all matching objects sequentially.

     (db/update-where! Table {:name table-name
                              :db_id (:id database)}
       :active false)

  This function favors convenience over performance; for updating truly massive numbers of objects (where you don't
  care about calling the usual `pre-update` and `post-update` hooks) consider using `execute!` directly."
  {:style/indent 2}
  ([model conditions changes]
   {:pre [(map? conditions) (map? changes)]}
   (when (seq changes)
     (when-let [matching-objects (seq (apply select model conditions))]
       (update! (for [object matching-objects]
                  (merge object changes))))))

  ([model conditions-map k v & more]
   (update-where! model conditions-map (u/varargs->map k v more))))


#_(defn update-non-nil-keys!
  "Like `update!`, but filters out key-value pairs with `nil` values."
  {:style/indent 2}
  ([model id kvs]
   (update! model id (into {} (for [[k v] kvs
                                     :when (not (nil? v))]
                                 [k v]))))
  ([model id k v & more]
   (update-non-nil-keys! model id (apply array-map k v more))))


;;;                                                      insert!
;;; ==================================================================================================================

#_(defn insert!
  "Insert a new object into the Database. Resolves `entity`, calls its `pre-insert` method on `row-map` to prepare
  it before insertion; after insert, it fetches and the newly created object, passes it to `post-insert`, and
  returns the results.

  For flexibility, `insert!` can handle either a single map or individual kwargs:

     (db/insert! Label {:name \"Toucan Unfriendly\"})
     (db/insert! 'Label :name \"Toucan Friendly\")"
  {:style/indent 1} ; TODO - not sure if want
  ([instance-or-instances]
   (if (sequential? instance-or-instances)
     (insert! (first instance-or-instances) instance-or-instances)
     (insert! instance-or-instances instance-or-instances)))

  ([model instance-or-instances]
   (when (seq instance-or-instances)
     (if (sequential? instance-or-instances)
       (seq (ops/insert! model instance-or-instances))
       (first (ops/insert! model [instance-or-instances])))))

  ([model k v & more]
   (insert! model (u/varargs->map k v more))))


;;;                                                      delete!
;;; ==================================================================================================================

;; TODO
#_(defn delete!
  "Delete of object(s). For each matching object, the `pre-delete` multimethod is called, which should do
  any cleanup needed before deleting the object, (such as deleting objects related to the object about to
  be deleted), or otherwise enforce preconditions before deleting (such as refusing to delete the object if
  something else depends on it).

     (delete! Database :id 1)"
  {:style/indent 1}
  ([instance-or-instances]
   (when (seq instance-or-instances)
     (if (sequential? instance-or-instances)
       (seq (ops/delete! (first instance-or-instances) instance-or-instances))
       (first (ops/delete! instance-or-instances [instance-or-instances])))))

  ([model & conditions]
   (delete! (apply select model conditions))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     save!                                                      |
;;; +----------------------------------------------------------------------------------------------------------------+

#_(defn save!
  ([instance-or-instances]
   (if (sequential? instance-or-instances)
     (save! (first instance-or-instances) instance-or-instances)
     (save! instance-or-instances instance-or-instances)))

  ([model instance-or-instances]
   (when (seq instance-or-instances)
     (if (sequential? instance-or-instances)
       (seq (ops/save! model instance-or-instances))
       (first (ops/save! model [instance-or-instances]))))))


;;; +----------------------------------------------------------------------------------------------------------------+
;;; |                                                     clone!                                                     |
;;; +----------------------------------------------------------------------------------------------------------------+

#_(defn clone!
  ([instance-or-instances]
   (if (sequential? instance-or-instances)
     (clone! (first instance-or-instances) instance-or-instances)
     (clone! instance-or-instances instance-or-instances)))

  ([model instance-or-instances]
   (when (seq instance-or-instances)
     (if (sequential? instance-or-instances)
       (seq (ops/clone! model instance-or-instances))
       (first (ops/clone! model [instance-or-instances]))))))
