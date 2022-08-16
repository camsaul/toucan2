(ns macros.toucan.models)

(defmacro defmodel
  [model _table-name]
  `(def ~model ~(keyword (name model))))
