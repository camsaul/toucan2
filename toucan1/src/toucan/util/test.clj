(ns toucan.util.test
  "Utility functions for writing tests with Toucan models."
  (:require [toucan2.tools.with-temp :as with-temp]))

(defmacro ^{:deprecated "2.0.0"} with-temp [modelable [binding properties] & body]
  `(with-temp/with-temp ~[modelable binding properties]
     ~@body))

(defmacro ^{:deprecated "2.0.0"} with-temp* [model-bindings & body]
  `(with-temp/with-temp ~(vec (mapcat
                               (fn [[modelable [binding properties]]]
                                 [modelable binding properties])
                               (partition-all 2 model-bindings)))
     ~@body))
