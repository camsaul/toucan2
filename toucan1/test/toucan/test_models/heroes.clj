(ns toucan.test-models.heroes
  "Empty namespace just so the model resolution stuff doesn't complain. Not 100% we need this."
  (:require [methodical.core :as m]
            [toucan2.test :as test]))

(m/defmethod test/create-table-sql-file [:default ::heroes]
  "Use the file resolution logic from the impl in [[toucan.test-setup]]."
  [db-type table-name]
  ((m/effective-method test/create-table-sql-file [:default :toucan1/model]) db-type table-name))
