(ns toucan.test-setup
  "Test setup logic and helper functions. All test namespaces should require this one to make sure the env is set up
  properly."
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan.test-models.address :refer [Address]]
   [toucan.test-models.category :refer [Category]]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.user :refer [User]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan2.connection :as conn]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(models/set-root-namespace! 'toucan.test-models)

(defn- reset-db! []
  (doseq [model [Address
                 Category
                 PhoneNumber
                 User
                 Venue]]
    (conn/with-connection [conn ::test/db]
      (test/create-table! test/*db-type* conn model))))

(def initialized? (atom false))

(defn- init-db! []
  (when-not @initialized?
    (reset-db!)
    (reset! initialized? true)))

(derive ::db ::test/db)

(m/defmethod model/default-connectable :toucan1/model
  [_model]
  ::db)

(m/defmethod conn/do-with-connection ::db
  [connectable f]
  (init-db!)
  (next-method connectable f))
