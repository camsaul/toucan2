(ns toucan.common)

(defn resolve-model [model]
  (cond (symbol? model)
    (keyword "models" (name model))

    (:toucan.models/model model)
    model

    (keyword? model)
    model

    :else
    (throw (ex-info (format "Not a valid model: %s" (pr-str model))
                    {:model model}))))
