(ns toucan2.tools.compile
  "Macros that can wrap a form and return the built query, compiled query, etc. without executing it."
  (:refer-clojure :exclude [compile])
  (:require
   [methodical.core :as m]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.execute :as execute]
   [toucan2.realize :as realize]))

(m/defmethod conn/do-with-connection ::compile
  [connectable f]
  (f connectable))

(m/defmethod execute/reduce-compiled-query-with-connection [::compile :default :default]
  [_connectable _model compiled-query rf init]
  (if (instance? clojure.lang.ITransientCollection init)
    (rf init {::query compiled-query})
    [{::query compiled-query}]))

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
  `(binding [conn/*current-connectable* ::compile]
     (let [query# (do ~@body)]
       (or (::query query#)
           (::query (realize/reduce-first query#))))))

(defn ^:no-doc identity-with-compiled-query
  "Impl for the [[build]] macro. Don't use this directly."
  [_model query f]
  (f query))

(defmacro build
  "Return the built query before compilation that would have been executed by `body` without compiling or executing it."
  {:style/indent 0}
  [& body]
  `(binding [compile/*with-compiled-query-fn* identity-with-compiled-query]
     (compile
       ~@body)))

;;; TODO

;; (defn ^:no-doc identity-with-resolved-query
;;   "Impl for the [[resolved]] macro. Don't use this directly."
;;   [model queryable f]
;;   (let [])
;;   (query/do-with-resolved-query model queryable identity))
;;
;; (defmacro resolved
;;   "Return the resolved query and parsed args *before* building a query (e.g. before creating a Honey SQL query from the
;;   args passed to [[toucan2.select/select]] created by `body` without building a query, compiling it, or executing it."
;;   {:style/indent 0}
;;   [& body]
;;   `(binding [query/*with-resolved-query-fn* identity-with-resolved-query]
;;      (build
;;        ~@body)))
