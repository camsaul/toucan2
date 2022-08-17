(ns macros.toucan2.tools.after-insert)

(defmacro define-after-insert
  [model [instance-binding] & body]
  `(do
     ~model
     (fn [~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~instance-binding]
       ~@body)))
