(ns toucan2.util.schema
  (:require [schema.core :as s]
            [toucan2.instance :as instance]))

(defn instance-of [model]
  (s/named
   (s/constrained
    toucan2.instance.IInstance
    (fn [instance]
      (isa? (instance/tableable instance) model)))
   (format "Toucan 2 instance of %s" (pr-str model))))
