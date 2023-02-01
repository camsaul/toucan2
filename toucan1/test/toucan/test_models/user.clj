(ns toucan.test-models.user
  "A very simple model for testing out basic DB functionality."
  (:require
   [toucan.models :as t1.models]))

(t1.models/defmodel User :t1_users)
