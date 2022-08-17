(ns macros.toucan2.tools.before-update)

(defmacro define-before-update
  [model [instance-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~instance-binding]
       ~@body)))
