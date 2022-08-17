(ns toucan.test-models.venue
  "A model with `:types`, custom `:properties`, and `:default-fields`."
  (:require
   [methodical.core :as m]
   [toucan.models :as models]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]))

(models/defmodel Venue :t1_venues)

(helpers/define-default-fields Venue
  [:id :name :category])

(defn- now [] (java.time.LocalDateTime/now))

(models/add-property! ::timestamped?
  :insert (fn [obj]
            (assoc obj :created-at (now), :updated-at (now)))
  :update (fn [obj]
            (assoc obj :updated-at (now))))

(derive Venue ::timestamped?)

(def keyword-transform
  {:in  (fn [k]
          (cond
            (not (keyword? k)) k
            (namespace k)      (str (namespace k) "/" (name k))
            :else              (name k)))
   :out keyword})

(helpers/deftransforms Venue
  {:category keyword-transform})

(m/defmethod test/create-table-sql-file Venue
  [_table-name]
  "toucan1/test/toucan/test_models/venue.sql")
