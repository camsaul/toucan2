(ns toucan2.tools.identity-query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.select :as select]))

(defrecord IdentityQuery [rows]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `identity-query rows))

  realize/Realize
  (realize [_this]
    (realize/realize rows))

  clojure.lang.IReduceInit
  (reduce [_ rf init]
    (reduce rf init rows)))

(defn identity-query
  "A queryable that returns `rows` as-is without compiling anything or running anything against a database.
  Good for mocking stuff."
  [reducible-rows]
  (->IdentityQuery reducible-rows))

(m/defmethod execute/reduce-uncompiled-query [:default IdentityQuery]
  [_connectable model {:keys [rows]} rf init]
  (transduce
   (map (if model
          (fn [row]
            (instance/instance model row))
          identity))
   (completing rf)
   init
   rows))

(m/defmethod query/build [::select/select :default IdentityQuery]
  [_query-type _model {:keys [query], :as _args}]
  query)
