(ns toucan.test-models.venue
  "A model with `:types`, custom `:properties`, and `:default-fields`."
  (:require
   [toucan.models :as t1.models]))

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
