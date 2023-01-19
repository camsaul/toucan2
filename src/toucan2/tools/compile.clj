(ns toucan2.tools.compile
  "Macros that can wrap a form and return the built query, compiled query, etc. without executing it."
  (:refer-clojure :exclude [compile])
  (:require
   [toucan2.pipeline :as pipeline]))

(defn ^:no-doc -compile
  "Impl for the [[compile]] macro. Do not use this directly."
  [thunk]
  (binding [pipeline/*transduce-execute* (fn [_rf _query-type _model compiled-query]
                                           compiled-query)]
    (thunk)))

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
  `(-compile (^:once fn* [] ~@body)))

(defn ^:no-doc -build
  "Impl for the [[build]] macro. Do not use this directly."
  [thunk]
  (binding [pipeline/*compile* (fn [_query-type _model built-query]
                                 built-query)]
    (-compile thunk)))

(defmacro build
  "Return the built query before compilation that would have been executed by `body` without compiling or executing it."
  {:style/indent 0}
  [& body]
  `(-build (^:once fn* [] ~@body)))

(defn -resolved [thunk]
  (binding [pipeline/*build* (fn [_query-type _model _parsed-args resolved-query]
                               resolved-query)]
    (-build thunk)))

(defmacro resolved
  "Return the resolved query and parsed args *before* building a query (e.g. before creating a Honey SQL query from the
  args passed to [[toucan2.select/select]] created by `body` without building a query, compiling it, or executing it."
  {:style/indent 0}
  [& body]
  `(-resolved (^:once fn* [] ~@body)))

;; (defmacro parsed-args
;;   [& body]
;;   )
