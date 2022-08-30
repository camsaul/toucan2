(ns toucan.test-models.venue
  "A model with `:types`, custom `:properties`, and `:default-fields`."
  (:require
   [methodical.core :as m]
   [toucan.models :as t1.models]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(t1.models/defmodel Venue :t1_venues)

(defn- now [] (java.time.LocalDateTime/now))

(t1.models/add-property! ::timestamped?
  :insert (fn [obj]
            (assoc obj :created-at (now), :updated-at (now)))
  :update (fn [obj]
            (assoc obj :updated-at (now))))

(t1.models/define-methods-with-IModel-method-map
 Venue
 {:default-fields (constantly [:id :name :category])
  :types          (constantly {:category :keyword})
  :properties     (constantly {::timestamped? true})})

(m/defmethod test/create-table-sql-file [:postgres Venue]
  [_db-type _table-name]
  "toucan/test_models/venue.postgres.sql")

(m/defmethod test/create-table-sql-file [:h2 Venue]
  [_db-type _table-name]
  "toucan/test_models/venue.h2.sql")
