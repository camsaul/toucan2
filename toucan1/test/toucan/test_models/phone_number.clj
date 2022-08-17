(ns toucan.test-models.phone-number
  "A model with a custom primary key."
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(models/defmodel PhoneNumber :t1_phone_numbers)

(m/defmethod model/primary-keys PhoneNumber
  [_model]
  :number)

(m/defmethod instance/key-transform-fn PhoneNumber
  [_model]
  identity)

(m/defmethod test/create-table-sql-file [:default PhoneNumber]
  [_db-type _table-name]
  "toucan1/test/toucan/test_models/phone_number.sql")
