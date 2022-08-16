(ns toucan.test-models.user
  "A very simple model for testing out basic DB functionality."
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan2.test :as test]))

(models/defmodel User :t1_users)

(m/defmethod test/create-table-sql-file User
  [_table-name]
  "toucan1/test/toucan/test_models/user.sql")
