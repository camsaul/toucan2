(ns toucan2.save
  (:require
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.update :as update]
   [toucan2.util :as u]))

(m/defmulti save!
  {:arglists '([object])}
  (fn [object]
    (u/dispatch-value (instance/model object))))

(m/defmethod save! :around :default
  [object]
  (try
    (u/with-debug-result (format "Save %s %s changes %s"
                                 (pr-str (instance/model object))
                                 (pr-str object)
                                 (pr-str (instance/changes object)))
      (next-method object))
    (catch Throwable e
      (throw (ex-info (format "Error saving %s: %s"
                              (pr-str (instance/model object))
                              (ex-message e))
                      {:model  (instance/model object)
                       :object object
                       :changes (instance/changes object)}
                      e)))))

(m/defmethod save! :default
  [object]
  (assert (instance/instance? object)
          (format "Don't know how to save something that's not a Toucan instance. Got: ^%s %s"
                  (some-> object class .getCanonicalName)
                  (pr-str object)))
  (if-let [changes (not-empty (instance/changes object))]
    (model/with-model [model (instance/model object)]
      (let [pk-values     (select-keys object (model/primary-keys (instance/model object)))
            rows-affected (update/update! model pk-values changes)]
        (when-not (pos? rows-affected)
          (throw (ex-info (format "Unable to save object: %s with primary key %s does not exist."
                                  (pr-str model)
                                  (pr-str pk-values))
                          {:object object
                           :pk     pk-values})))
        (when (> rows-affected 1)
          (u/println-debug "Warning: more than 1 row affected when saving %s with primary key %s"
                           (pr-str model) (pr-str pk-values)))
        (instance/reset-original object)))
    object))
