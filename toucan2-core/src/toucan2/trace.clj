(ns toucan2.trace
  (:require [methodical.util.trace :as m.trace]
            [toucan2.mutative :as mutative]))

(defn do-trace-updates [thunk]
  (binding [mutative/*update!* (partial m.trace/trace* (vary-meta mutative/update!* assoc ::m.trace/description 'update!*))]
    (thunk)))

(defmacro trace-updates {:style/indent 0} [& body]
  `(do-trace-updates (fn [] ~@body)))
