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
    rows))

(defn identity-query
  "A queryable that returns `rows` as-is without compiling anything or running anything against a database.
  Good for mocking stuff."
  [rows]
  (->IdentityQuery rows))

(m/defmethod execute/reduce-uncompiled-query [:default IdentityQuery]
  [_connectable model {:keys [rows]} rf init]
  (reduce
   rf
   init
   (cond->> rows
     model (eduction (map (fn [row]
                            (instance/instance model row)))))))

(m/defmethod query/build [::select/select :default IdentityQuery]
  [_query-type _model {:keys [query], :as _args}]
  query)
