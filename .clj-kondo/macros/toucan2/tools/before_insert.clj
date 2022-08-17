(ns macros.toucan2.tools.before-insert)

(defmacro define-before-insert
  [model [instance-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~instance-binding]
       ~@body)))
