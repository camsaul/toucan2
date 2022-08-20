(ns toucan.util.test
  "Utility functions for writing tests with Toucan models."
  (:require [toucan2.tools.with-temp :as with-temp]))

(defmacro with-temp [modelable [binding properties] & body]
  `(with-temp/with-temp ~[modelable binding properties]
     ~@body))

(defmacro with-temp* [model-bindings & body]
  `(with-temp/with-temp ~(vec (mapcat
                               (fn [[modelable [binding properties]]]
                                 [modelable binding properties])
                               (partition-all 2 model-bindings)))
     ~@body))

(defn with-temp-defaults
  [model]
  (with-temp/with-temp-defaults model))
