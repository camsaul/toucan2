(ns toucan.test-models.address
  (:require
   [methodical.core :as m]
   [toucan.models :as t1.models]
   [toucan2.test :as test]))

(t1.models/defmodel Address :t1_address)

(m/defmethod test/create-table-sql-file [:default Address]
  [_db-type _table-name]
  "toucan/test_models/address.sql")
