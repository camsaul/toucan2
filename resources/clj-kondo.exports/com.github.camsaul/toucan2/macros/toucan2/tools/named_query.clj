(ns macros.toucan2.tools.named-query)

;;; separate function because recursive macroexpansion doesn't seem to work.
(defn- define-named-query*
  [query-name query-type model resolved-query]
  `(do
     ~query-name
     ~query-type
     ~model
     (fn [~(vary-meta '&query-type assoc :clj-kondo/ignore [:unused-binding])
          ~(vary-meta '&model assoc :clj-kondo/ignore [:unused-binding])
          ~(vary-meta '&parsed-args assoc :clj-kondo/ignore [:unused-binding])
          ~(vary-meta '&unresolved-query assoc :clj-kondo/ignore [:unused-binding])]
       ~resolved-query)))

(defmacro define-named-query
  ([query-name resolved-query]
   (define-named-query* query-name :default :default resolved-query))

  ([query-name query-type model resolved-query]
   (define-named-query* query-name query-type model resolved-query)))
