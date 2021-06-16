(ns toucan.test-models.venue
  "A model with `:types`, custom `:properties`, and `:default-fields`."
  (:require [methodical.core :as m]
            [toucan.models :as models]
            [toucan2.helpers :as helpers]
            [toucan2.select :as select]
            [toucan2.util :as u])
  (:import java.sql.Timestamp))

(defn- now [] (Timestamp. (System/currentTimeMillis)))

(models/add-property! :timestamped?
  :insert (fn [obj _]
            (assoc obj :created-at (now), :updated-at (now)))
  :update (fn [obj _]
            (assoc obj :updated-at (now))))

(models/defmodel Venue :t1_venues)

(derive :models/Venue :toucan1.models.properties/timestamped?)

;; TODO -- `define-default-fields` helper?
(m/defmethod select/select* [:default :models/Venue :toucan2.honeysql/select-query]
  [connectable tableable query options]
  (let [query (cond-> query
                (= (:select query) [:*])
                (assoc :select [:id :name :category]))]
    (next-method connectable tableable query options)))

(helpers/deftransforms :models/Venue
  {:category {:in  #(some-> % u/qualified-name)
              :out keyword}})
