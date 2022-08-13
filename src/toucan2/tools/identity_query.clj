(ns toucan2.tools.identity-query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.current :as current]
   [toucan2.instance :as instance]
   [toucan2.compile :as compile]))

#_(m/defmethod compile/compile* [:default :default :toucan/identity-query]
  [_ _ rows _]
  rows)

#_(m/defmethod compile/from* [:default :default :toucan/identity-query]
  [_ _ rows _]
  rows)

(defrecord IdentityQuery [rows]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `identity-query rows))

  realize/Realize
  (realize [_this]
    rows))

(defn identity-query
  "A queryable that returns `rows` as-is without compiling anything or running anything against a database.
  Good for mocking stuff."
  [rows]
  (->IdentityQuery rows))

(m/defmethod compile/do-with-compiled-query IdentityQuery
  [query f]
  (f query))

(m/defmethod query/reduce-query IdentityQuery
  [_connectable {:keys [rows]} rf init]
  (reduce
   rf
   init
   (if current/*model*
     (eduction (map (fn [row]
                      (instance/instance current/*model* row)))
               rows)
     rows)))

(m/defmethod select/build-query [:default IdentityQuery]
  [_model query _columns _conditions]
  query)
