(ns bluejdbc.util.schema
  (:require [bluejdbc.instance :as instance]
            [schema.core :as s]))

(defn instance-of [model]
  (s/named
   (s/constrained
    bluejdbc.instance.IInstance
    (fn [instance]
      (isa? (instance/tableable instance) model)))
   (format "Blue JDBC instance of %s" (pr-str model))))
