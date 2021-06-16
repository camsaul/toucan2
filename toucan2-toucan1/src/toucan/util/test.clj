(ns toucan.util.test
  "Utility functions for writing tests with Toucan models."
  (:require [toucan2.tools.with-temp :as with-temp]))

(defmacro ^:deprecated with-temp [tableable [binding properties] & body]
  `(with-temp/with-temp ~[tableable binding properties]
     ~@body))

(defmacro ^:deprecated with-temp* [model-bindings & body]
  `(with-temp/with-temp ~(vec (mapcat
                               (fn [[tableable [binding properties]]]
                                 [tableable binding properties])
                               (partition-all 2 model-bindings)))
     ~@body))
