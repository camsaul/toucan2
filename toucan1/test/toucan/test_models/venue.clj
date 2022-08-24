(ns toucan.test-models.venue
  "A model with `:types`, custom `:properties`, and `:default-fields`."
  (:require
   [methodical.core :as m]
   [toucan.models :as t1.models]
   [toucan2.test :as test]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.tools.transformed :as transformed]))

(set! *warn-on-reflection* true)

(t1.models/defmodel Venue :t1_venues)

(default-fields/define-default-fields Venue
  [:id :name :category])

(defn- now [] (java.time.LocalDateTime/now))

(t1.models/add-property! ::timestamped?
  :insert (fn [obj]
            (assoc obj :created-at (now), :updated-at (now)))
  :update (fn [obj]
            (assoc obj :updated-at (now))))

#_(derive Venue ::timestamped?)

(t1.models/defproperties Venue {::timestamped? true})

(def keyword-transform
  {:in  (fn [k]
          (cond
            (not (keyword? k)) k
            (namespace k)      (str (namespace k) "/" (name k))
            :else              (name k)))
   :out keyword})

(transformed/deftransforms Venue
  {:category keyword-transform})

(m/defmethod test/create-table-sql-file [:postgres Venue]
  [_db-type _table-name]
  "toucan/test_models/venue.postgres.sql")

(m/defmethod test/create-table-sql-file [:h2 Venue]
  [_db-type _table-name]
  "toucan/test_models/venue.h2.sql")
