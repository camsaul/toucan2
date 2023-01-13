(ns toucan2.tools.debug
  (:require
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]))

(defn- print-result [message result]
  (log/pprint-doc (log/->Doc [(log/->Text message) result]))
  result)

(defn -debug [thunk]
  (binding [pipeline/*build*   (comp (partial print-result "\nBuilt:")
                                     (let [build* pipeline/*build*]
                                       (fn [query-type model parsed-args resolved-query]
                                         (print-result "\nParsed args:" parsed-args)
                                         (print-result "\nResolved query:" resolved-query)
                                         (build* query-type model parsed-args resolved-query))))
            pipeline/*compile* (comp (partial print-result "\nCompiled:") pipeline/*compile*)]
    (thunk)))

(defmacro debug
  "Simple debug macro. This is a placeholder until I come up with a more sophisticated version."
  {:style/indent 0}
  [& body]
  `(-debug (^:once fn* [] ~@body)))
