(ns toucan2.tools.before-insert
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(m/defmulti before-insert
  {:arglists '([model row])}
  u/dispatch-on-first-arg)

(defn- do-before-insert-to-rows [rows model]
  (mapv
   (fn [row]
     (u/try-with-error-context [`before-insert {::model model, ::row row}]
       (u/with-debug-result ["Do before-insert to %s" row]
         (before-insert model row))))
   rows))

(m/defmethod pipeline/transduce-with-model* :before [:toucan.query-type/insert.* ::before-insert]
  [_rf _query-type model parsed-args]
  (update parsed-args :rows do-before-insert-to-rows model))

;;; Important! before-insert should be done BEFORE any [[toucan2.tools.transformed/transforms]]. Transforms are often
;;; for serializing and deserializing values; we don't want before insert methods to have to work with
;;; already-serialized values.
;;;
;;; By marking `::before-insert` as preferred over `:toucan2.tools.transformed/transformed` it will be done first (see
;;; https://github.com/camsaul/methodical#before-methods)
(m/prefer-method! #'pipeline/transduce-with-model*
                  [:toucan.query-type/insert.* ::before-insert]
                  [:toucan.query-type/insert.* :toucan2.tools.transformed/transformed])

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
