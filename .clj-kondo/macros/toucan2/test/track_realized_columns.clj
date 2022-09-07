(ns macros.toucan2.test.track-realized-columns)

(defmacro with-realized-columns [[binding] & body]
  `(let [~binding (fn [])]
     ~@body))
