(ns hooks.toucan2.tools.after-insert
  (:require
   [clj-kondo.hooks-api :as hooks]
   [hooks.toucan2.common]))

(defn define-after-insert [context]
  (letfn [(update-node [node]
            (let [[_ dispatch-value binding-node & body] (:children node)]
              (-> (hooks/list-node
                   [(hooks/token-node `do)
                    dispatch-value
                    (hooks/list-node
                     (list*
                      (hooks/token-node `fn)
                      (hooks/vector-node
                       [(hooks/token-node '&model)
                        (first (:children binding-node))])
                      (-> body
                          (hooks.toucan2.common/splice-into-body (hooks/token-node '&model)))))])
                  (with-meta (meta node)))))]
    (update context :node update-node)))

(comment
  (as-> '(after-insert/define-after-insert ::venues.after-insert.composed
           [venue]
           {:pre [(map? venue)], :post [(map? %)]}
           (assoc venue :composed? true)) <>
    (pr-str <>)
    (hooks/parse-string <>)
    (define-after-insert {:node <>})
    (:node <>)
    (hooks/sexpr <>)))
