(ns toucan.test-models.phone-number
  "A model with a custom primary key."
  (:require
   [methodical.core :as m]
   [toucan.models :as t1.models]
   [toucan2.model :as model]))

(t1.models/defmodel PhoneNumber :t1_phone_numbers)

(m/defmethod model/primary-keys PhoneNumber
  [_model]
  :number)
