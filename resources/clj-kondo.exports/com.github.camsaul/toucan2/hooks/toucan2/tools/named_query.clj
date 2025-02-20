(ns hooks.toucan2.tools.named-query
  (:require
   [clj-kondo.hooks-api :as hooks]))

;;; separate function because recursive macroexpansion doesn't seem to work.
(defn- define-named-query*
  [node]
  (let [[query-name query-type model resolved-query]
        (if (= (count (:children node)) 3)
          (let [[_ query-name resolved-query] (:children node)]
            [query-name :default :default resolved-query])
          (:children node))]
    (-> (hooks/list-node
         [(hooks/token-node `do)
          query-name
          query-type
          model
          (hooks/list-node
           [(hooks/token-node `fn)
            (hooks/vector-node
             [(hooks/token-node '&query-type)
              (hooks/token-node '&model)
              (hooks/token-node '&parsed-args)
              (hooks/token-node '&unresolved-query)])
          ;; make these appear used
            (hooks/token-node '&query-type)
            (hooks/token-node '&model)
            (hooks/token-node '&parsed-args)
            (hooks/token-node '&unresolved-query)
            resolved-query])])
        (with-meta (meta node)))))

(defn define-named-query [context]
  (update context :node define-named-query*))

(comment
  (as-> '(named-query/define-named-query ::people.named-conditions
           {:id [:> 1]}) <>
    (pr-str <>)
    (hooks/parse-string <>)
    (define-named-query {:node <>})
    (:node <>)
    (hooks/sexpr <>)))
