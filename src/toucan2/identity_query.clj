(ns toucan2.identity-query
  (:require [methodical.core :as m]
            [toucan2.build-query :as build-query]
            [toucan2.compile :as compile]
            [toucan2.query :as query]
            [toucan2.util :as u]))

(derive :toucan2/identity-query :toucan2/query)

(m/defmethod compile/compile* [:default :default :toucan2/identity-query]
  [_ _ rows _]
  rows)

;; (m/defmethod compile/from* [:default :default :toucan2/identity-query]
;;   [_ _ rows _]
;;   rows)

(m/defmethod query/reducible-query* [:default :default :toucan2/identity-query]
  [_ _ rows _]
  (u/unwrap-dispatch-on rows))

(defn identity-query
  "A queryable that returns `rows` as-is without compiling anything or running anything against a database.
  Good for mocking stuff."
  [rows]
  (u/dispatch-on rows :toucan2/identity-query))
