(ns toucan2.tools.compile
  "Macros that can wrap a form and return the built query, compiled query, etc. without executing it."
  (:refer-clojure :exclude [compile])
  (:require
   [toucan2.pipeline :as pipeline]))

(defmacro compile
  "Return the compiled query that would be executed by a form, rather than executing that form itself.

  ```clj
  (compile
    (delete/delete :table :id 1))
  =>
  [\"DELETE FROM table WHERE ID = ?\" 1]
  ```"
  {:style/indent 0}
  [& body]
  `(binding [pipeline/*transduce-compiled-query* (fn [rf# query-type# model# compiled-query#]
                                                   compiled-query#)]
     ~@body))

(defmacro build
  "Return the built query before compilation that would have been executed by `body` without compiling or executing it."
  {:style/indent 0}
  [& body]
  `(binding [pipeline/*transduce-built-query* (fn [rf# query-type# model# built-query#]
                                                built-query#)]
     ~@body))

(defmacro resolved
  "Return the resolved query and parsed args *before* building a query (e.g. before creating a Honey SQL query from the
  args passed to [[toucan2.select/select]] created by `body` without building a query, compiling it, or executing it."
  {:style/indent 0}
  [& body]
  `(binding [pipeline/*transduce-resolved-query* (fn [rf# query-type# model# parsed-args# resolved-query#]
                                                   {:parsed-args parsed-args#, :resolved-query resolved-query#})]
     ~@body))

;; (defmacro parsed-args
;;   [& body]
;;   )
