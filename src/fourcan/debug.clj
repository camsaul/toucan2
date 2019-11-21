(ns fourcan.debug)

(def ^:dynamic *debug* false)

(defmacro debug {:style/indent 0} [& body]
  `(binding [*debug* true]
   ~@body))
