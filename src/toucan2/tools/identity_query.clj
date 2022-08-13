(ns toucan2.tools.identity-query)

;; (derive :toucan/identity-query :toucan2/query)

;; (m/defmethod compile/compile* [:default :default :toucan/identity-query]
;;   [_ _ rows _]
;;   rows)

;; ;; (m/defmethod compile/from* [:default :default :toucan/identity-query]
;; ;;   [_ _ rows _]
;; ;;   rows)

;; (m/defmethod query/reducible-query* [:default :default :toucan/identity-query]
;;   [_ _ rows _]
;;   (u/unwrap-dispatch-on rows))

;; (defn identity-query
;;   "A queryable that returns `rows` as-is without compiling anything or running anything against a database.
;;   Good for mocking stuff."
;;   [rows]
;;   (u/dispatch-on rows :toucan/identity-query))
