(ns hooks.toucan2.tools.after-select
  (:require
   [clj-kondo.hooks-api :as hooks]
   [hooks.toucan2.common]))

(defn define-after-select [context]
  (letfn [(update-node [node]
            (let [[_ dispatch-value binding-node & body] (:children node)]
              (-> (hooks/list-node
                   [(hooks/token-node `do)
                    dispatch-value
                    (hooks/list-node
                     (list*
                      (hooks/token-node `fn)
                      (hooks/vector-node
                       [(hooks/token-node '&query-type)
                        (hooks/token-node '&model)
                        (hooks/token-node '&parsed-args)
                        (first (:children binding-node))])
                      ;; make these appear used.
                      (-> body
                          (hooks.toucan2.common/splice-into-body (hooks/token-node '&query-type)
                                                                 (hooks/token-node '&model)
                                                                 (hooks/token-node '&parsed-args)))))])
                  (with-meta (meta node)))))]
    (update context :node update-node)))

(comment
  (as-> '(t2/define-after-select ::people
           [person]
           (assoc person :cool-name (str "Cool " (:name person)))) <>
    (pr-str <>)
    (hooks/parse-string <>)
    (define-after-select {:node <>})
    (:node <>)
    (hooks/sexpr <>)))
