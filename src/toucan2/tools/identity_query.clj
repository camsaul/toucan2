(ns toucan2.tools.identity-query
  (:require
   [methodical.core :as m]
   [pretty.core :as pretty]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.pipeline :as pipeline]
   [toucan2.realize :as realize]))

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
    (log/debugf :execute "reduce IdentityQuery rows")
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

(m/defmethod pipeline/transduce-execute [#_query-type :default #_model :default #_query IdentityQuery]
  [rf _query-type model {:keys [rows], :as _query}]
  (log/debugf :execute "transduce IdentityQuery rows %s" rows)
  (transduce (if model
               (map (fn [result-row]
                      (instance/instance model result-row)))
               identity)
             rf
             rows))

(m/defmethod pipeline/compile [#_query-type :default #_model :default #_query IdentityQuery]
  [_query-type _model query]
  query)

(m/defmethod pipeline/build [#_query-type :default #_model :default #_query IdentityQuery]
  "This is an around method so we can intercept anything else that might normally be considered a more specific method
  when it dispatches off of more-specific values of `query-type`."
  [_query-type _model _parsed-args resolved-query]
  resolved-query)

(m/defmethod pipeline/transduce-query [#_query-type     :default
                                       #_model          IdentityQuery
                                       #_resolved-query :default]
  "Allow using an identity query as an 'identity model'."
  [rf _query-type model _parsed-args _resolved-query]
  (transduce identity rf model))


;;;; Identity connection

(defrecord ^:no-doc IdentityConnection [rows]
  pretty/PrettyPrintable
  (pretty [_this]
    (list `identity-connection rows)))

(m/defmethod conn/do-with-connection IdentityConnection
  [connectable f]
  (f connectable))

(m/defmethod conn/do-with-transaction IdentityConnection
  [connectable _options f]
  (f connectable))

(m/defmethod pipeline/transduce-execute-with-connection [#_conn IdentityConnection #_query-type :default #_model :default]
  [rf conn _query-type model _compiled-query]
  (transduce
   (map (fn [row]
          (instance/instance model row)))
   rf
   (:rows conn)))

(defn identity-connection [rows]
  (->IdentityConnection rows))

;; (defrecord ^:no-doc IdentityQuery2 [rows]
;;   pretty/PrettyPrintable
;;   (pretty [_this]
;;     (list `identity-query-2 rows)))

;; (defn identity-query-2 [rows]
;;   (->IdentityQuery2 rows))
