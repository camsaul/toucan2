(ns toucan.test-models.phone-number
  (:require [methodical.core :as m]
            [toucan.models :as models]
            [toucan2.tableable :as tableable]))

(models/defmodel PhoneNumber :t1_phone_numbers)

(m/defmethod tableable/primary-key* [:default :models/PhoneNumber]
  [_ _]
  :number)
