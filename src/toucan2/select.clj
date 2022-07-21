(ns toucan2.select
  (:require [clojure.spec.alpha :as s]
            [methodical.core :as m]
            [toucan2.util :as u]
            [toucan2.model :as model]
            [toucan2.connection :as conn]
            [toucan2.compile :as compile]))

(s/def ::default-select-args
  (s/cat ;; :modelable  (s/or
         ;;              :model          (complement sequential?)
         ;;              :model-and-cols (s/cat
         ;;                               :model any?
         ;;                               :cols  (s/* any?)))
         :conditions (s/* (s/cat
                           :k keyword?
                           :v (complement map?)))
         :query      (s/? any?)
         :options    (s/? map?)))

(defn parse-select-args [args]
  (let [parsed (s/conform ::default-select-args args)]
    (when (s/invalid? parsed)
      (throw (ex-info (format "Don't know how to interpret select args: %s" (s/explain-str ::default-select-args args))
                      (s/explain-data ::default-select-args args))))
    (cond-> parsed
      (nil? (:query parsed))     (assoc :query {})
      (seq (:conditions parsed)) (update :conditions (fn [conditions]
                                                       (into {} (map (juxt :k :v)) conditions))))))

(m/defmulti select-reducible*
  {:arglists '([model columns args])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod select-reducible* :default
  [model columns args]
  (let [{:keys [conditions query options]} (parse-select-args args)
        query                              (model/build-select-query model query columns conditions options)
        connectable                        (model/default-connectable model options)]
    (model/reducible-model-query connectable model query)))

(defn select-reducible [modelable & args]
  {:arglists '([modelable & conditions? compileable? options?]
               [[modelable & columns] & conditions? compileable? options?])}
  (let [[modelable & columns] (if (sequential? modelable)
                                modelable
                                [modelable])]
    (model/with-model [model modelable]
      (select-reducible* model columns args))))

(defn select
  {:arglists '([modelable & conditions? compileable? options?]
               [[modelable & columns] & conditions? compileable? options?])}
  [modelable & args]
  (let [reducible-query (apply select-reducible modelable args)]
    (reduce conj [] reducible-query)
    #_(into [] reducible-query)))
