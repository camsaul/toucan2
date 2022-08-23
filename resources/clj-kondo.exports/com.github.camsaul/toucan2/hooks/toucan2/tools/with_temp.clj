(ns hooks.toucan2.tools.with-temp
  (:require [clj-kondo.hooks-api :as hooks]))

(defn with-temp [{{[_ {bindings :children} & body] :children, :as node} :node}]
  #_(println "\n\nBEFORE\n")
  #_(clojure.pprint/pprint (hooks/sexpr node))
  (let [bindings* (into
                   []
                   (comp (partition-all 3)
                         (mapcat (fn [[model binding attributes]]
                                   (let [binding    (or binding (hooks/token-node '_))
                                         attributes (or attributes (hooks/token-node 'nil))]
                                     [binding (hooks/list-node
                                               (list
                                                (hooks/token-node 'do)
                                                model
                                                attributes))]))))
                   bindings)
        node*         (hooks/list-node
                       (list*
                        (hooks/token-node 'let)
                        (hooks/vector-node bindings*)
                        body))]
    #_(println "\n\nAFTER\n")
    #_(clojure.pprint/pprint (hooks/sexpr node*))
    {:node node*}))
