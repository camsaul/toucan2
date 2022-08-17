(ns macros.toucan2.test)

(defmacro do-db-types
  [[db-type-binding pred] & body]
  `(let [~db-type-binding ~pred]
     ~@body))
