(ns toucan.test-models.user
  "A very simple model for testing out basic DB functionality."
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan2.test :as test]))

(models/defmodel User :t1_users)

(m/defmethod test/create-table-sql-file [:postgres User]
  [_db-type _table-name]
  "toucan/test_models/user.postgres.sql")

(m/defmethod test/create-table-sql-file [:h2 User]
  [_db-type _table-name]
  "toucan/test_models/user.h2.sql")
