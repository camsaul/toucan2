(ns toucan2.model
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.query :as query]
   [toucan2.util :as u]))

(m/defmulti do-with-model
  {:arglists '([modelable f])}
  u/dispatch-on-keyword-or-type-1)

(m/defmethod do-with-model :default
  [model f]
  (f model))

(defmacro with-model [[model-binding modelable] & body]
  `(do-with-model ~modelable (^:once fn* [~model-binding] ~@body)))

(defrecord ReducibleModelQuery [connectable modelable compileable]
  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce
     rf
     init
     (with-model [model modelable]
       (eduction
        (map (fn [row]
               (list 'magic-map model (into {} row))))
        (query/reducible-query connectable compileable)))))

  pretty/PrettyPrintable
  (pretty [_this]
    (list `reducible-model-query connectable modelable compileable)))

(defn reducible-model-query [connectable modelable compileable]
  (->ReducibleModelQuery connectable modelable compileable))
