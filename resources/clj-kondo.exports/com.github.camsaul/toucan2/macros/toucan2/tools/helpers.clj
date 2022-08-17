(ns macros.toucan2.tools.helpers)

(defmacro define-before-select [dispatch-value [args-binding] & body]
  `(do
     ~dispatch-value
     (fn [~(vary-meta '&query-type assoc :clj-kondo/ignore [:unused-binding])
          ~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~args-binding]
       ~@body)))

(defmacro define-after-select-reducible [model [reducible-query-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~reducible-query-binding]
       ~@body)))

(defmacro define-after-select-each [model [instance-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~instance-binding]
       ~@body)))

(defmacro define-before-delete
  [model [instance-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~instance-binding]
       ~@body)))
