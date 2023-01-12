(ns toucan2.tools.before-insert
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(m/defmulti before-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(defn- do-before-insert-to-rows [rows model]
  (mapv
   (fn [row]
     (u/try-with-error-context [`before-insert {::model model, ::row row}]
       (log/tracef :compile "Do before-insert for %s %s" model row)
       (let [result (before-insert model row)]
         (log/tracef :compile "[before insert] => %s" row)
         result)))
   rows))

;;; make sure we transform rows whether it's in the parsed args or in the resolved query.

(m/defmethod pipeline/transduce-with-model :around [#_query-type :toucan.query-type/insert.*
                                                    #_model      ::before-insert]
  [rf query-type model parsed-args]
  (conn/with-transaction [_conn
                          (or conn/*current-connectable*
                              (model/default-connectable model))
                          {:nested-transaction-rule :ignore}]
    (let [parsed-args (cond-> parsed-args
                        (:rows parsed-args) (update :rows do-before-insert-to-rows model))]
      (next-method rf query-type model parsed-args))))

(m/defmethod pipeline/build :before [#_query-type :toucan.query-type/insert.*
                                     #_model      ::before-insert
                                     #_query      clojure.lang.IPersistentMap]
  [_query-type model _parsed-args resolved-query]
  (cond-> resolved-query
    (:rows resolved-query) (update :rows do-before-insert-to-rows model)))

;;; Important! before-insert should be done BEFORE any [[toucan2.tools.transformed/transforms]]. Transforms are often
;;; for serializing and deserializing values; we don't want before insert methods to have to work with
;;; already-serialized values.
;;;
;;; By marking `::before-insert` as preferred over `:toucan2.tools.transformed/transformed` it will be done first (see
;;; https://github.com/camsaul/methodical#before-methods)
(m/prefer-method! #'pipeline/transduce-with-model
                  [:toucan.query-type/insert.* ::before-insert]
                  [:toucan.query-type/insert.* :toucan2.tools.transformed/transformed.model])

(defmacro define-before-insert
  {:style/indent :defn}
  [model [instance-binding] & body]
  `(let [model# ~model]
     (u/maybe-derive model# ::before-insert)
     (m/defmethod before-insert model#
       [~'&model ~instance-binding]
       (cond->> (do ~@body)
         ~'next-method (~'next-method ~'&model)))))

(s/fdef define-before-insert
  :args (s/cat :model    some?
               :bindings (s/spec (s/cat :instance :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)
