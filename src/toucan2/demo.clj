(ns toucan2.demo
  (:require [toucan2.core :as t2]
            [methodical.core :as m]
            [clojure.string :as str]
            [toucan2.query :as query]
            [toucan2.select :as select]))

(m/defmethod t2/do-with-connection ::my-connection
  [_connectable f]
  (t2/do-with-connection "jdbc:postgresql:metabase?user=cam&password=cam" f))

(comment
  (t2/query ::my-connection ["SELECT * FROM metabase_database WHERE name = 'test-data';"]))

;;; define a model

(m/defmethod t2/default-connectable ::model
  [_model]
  ::my-connection)

(derive :models/database ::model)

(m/defmethod t2/table-name :models/database
  [_model]
  "metabase_database")

(comment
  (t2/select :models/database)

  (t2/select-one-fn :name :models/database)

  (t2/select-one-fn (comp str/lower-case :name) :models/database))

;;; derived models

(derive ::super-cool-database :models/database)

(t2/define-after-select ::super-cool-database
  [database]
  (assoc database :super-cool? true))

(comment
  (t2/select-one ::super-cool-database))

;;; build

(comment
  (t2/compile
    (t2/select-one ::super-cool-database))

  (t2/build
    (t2/select-one ::super-cool-database)))

;; SQL

(comment
  (t2/select-one ::super-cool-database ["SELECT * FROM metabase_database WHERE id = 3065"]))

;; HoneySQL

(comment
  (t2/select-one ::super-cool-database {:select [:*], :from [:metabase_database], :where [:= :id 3065]}))

;; named query

(m/defmethod toucan2.query/do-with-resolved-query [#_model :default ::named-query]
  [_model _queryable f]
  (f {:select [:*], :from [:metabase_database], :where [:= :id 3065]}))

(comment
  (t2/select-one :models/database ::named-query)

  (t2/select-one ::super-cool-database ::named-query))

;; magic
(comment
  (:updated_at (t2/select-one ::super-cool-database)))

(comment
  (assoc (t2/select-one ::super-cool-database) :updated_at "NOW!")

  (let [m (assoc (t2/select-one ::super-cool-database) :updated_at "NOW!")]
    (t2/changes m))

  ;; UPDATE
  (let [m (assoc (t2/select-one ::super-cool-database) :updated_at "NOW!")]
    (t2/save! m)))

;;; TABLE

(derive :models/table ::model)

(m/defmethod t2/table-name :models/table
  [_model]
  "metabase_table")

(comment
  (t2/select-one :models/table))

;; model with JOIN

(derive ::table-with-database :models/table)

(m/defmethod query/build :after [#_query-type ::select/select
                                 #_model ::table-with-database
                                 #_query clojure.lang.IPersistentMap]
  [_query-type _model honeysql]
  (assoc honeysql :left-join [[:metabase_database :db]
                              [:= :metabase_table.db_id :db.id]]))

(comment
  (t2/build (t2/select-one ::table-with-database))

  (t2/compile (t2/select-one ::table-with-database))

  (binding [toucan2.util/*debug* true]
    (t2/select-one ::table-with-database))

  ;; WHERE IS ALL MY STUFF (BROKEN)
  (t2/select-one ::table-with-database))
