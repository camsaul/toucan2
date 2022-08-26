(ns toucan2.tools.identity-query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.realize :as realize]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(defrecord ^:no-doc IdentityQuery [rows]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `identity-query rows))

  realize/Realize
  (realize [_this]
    (realize/realize rows))

  clojure.lang.IReduceInit
  (reduce [_this rf init]
    (u/with-debug-result ["reduce IdentityQuery rows"]
      (reduce rf init rows))))

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

(m/defmethod pipeline/transduce-built-query* [:toucan.result-type/instances :default IdentityQuery]
  [rf _query-type model {:keys [rows], :as _query}]
  (u/with-debug-result ["transduce IdentityQuery rows %s" rows]
    (transduce (if model
                 (map (fn [result-row]
                        (instance/instance model result-row)))
                 identity)
               rf
               rows)))

(m/defmethod query/build [:toucan.query-type/select.instances :default IdentityQuery]
  [_query-type _model {:keys [query], :as _args}]
  query)

(m/defmethod query/build [:default :default IdentityQuery]
  [_query-type _model {:keys [query], :as _args}]
  query)

(m/defmethod query/build [:default IdentityQuery :default]
  [_query-type _model {:keys [query], :as _args}]
  query)

;;; allow using an identity query as an 'identity model'
(m/defmethod pipeline/transduce-with-model* [:default IdentityQuery]
  [rf _query-type model _parsed-args]
  (transduce identity rf model))
