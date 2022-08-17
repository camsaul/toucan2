(ns toucan.test-models.heroes
  (:require [methodical.core :as m]
            [toucan2.test :as test]))

(m/defmethod test/create-table-sql-file [:default ::heroes]
  [_db-type _table-name]
  "toucan1/test/toucan/test_models/heroes.sql")
