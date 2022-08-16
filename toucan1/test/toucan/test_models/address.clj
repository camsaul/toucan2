(ns toucan.test-models.address
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan2.test :as test]))

(models/defmodel Address :t1_address)

(m/defmethod test/create-table-sql-file Address
  [_table-name]
  "toucan1/test/toucan/test_models/address.sql")
