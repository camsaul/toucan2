(ns toucan2.tools.identity-query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.operation :as op]
   [toucan2.query :as query]
   [toucan2.realize :as realize]))

(defrecord ^:no-doc IdentityQuery [rows]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `identity-query rows))

  realize/Realize
  (realize [_this]
    (realize/realize rows))

  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (reduce rf init rows)))

(defn identity-query
  "A queryable that returns `reducible-rows` as-is without compiling anything or running anything against a database.
  Good for mocking stuff.

  ```clj
  (def parrot-query
    (identity-query [{:id 1, :name \"Parroty\"}
                     {:id 2, :name \"Green Friend\"}]))

  (select/select ::parrot parrot-query)
  =>
  [(instance ::parrot {:id 1, :name \"Parroty\"})
   (instance ::parrot {:id 2, :name \"Green Friend\"})]
  ```"
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

(m/defmethod query/build [:toucan2.select/select :default IdentityQuery]
  [_query-type _model {:keys [query], :as _args}]
  query)

;;; allow using an identity query as an 'identity model'
(m/defmethod op/reducible-returning-instances* [:toucan2.select/select IdentityQuery]
  [_query-type identity-query _parsed-args]
  identity-query)
