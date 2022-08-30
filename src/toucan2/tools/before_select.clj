(ns toucan2.tools.before-select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.pipeline :as pipeline]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(defn ^:no-doc do-before-select
  "Impl for [[define-before-select]]. Don't call this directly."
  [model thunk]
  (u/try-with-error-context ["before select" {::model model}]
    (u/with-debug-result ["%s %s" `define-before-select model]
      (thunk))))

(defmacro define-before-select
  {:style/indent :defn}
  [model [args-binding] & body]
  `(m/defmethod pipeline/transduce-with-model :before [:toucan.query-type/select.* ~model]
     [rf# ~'&query-type ~'&model parsed-args#]
     (let [~args-binding parsed-args#]
       (do-before-select ~'&model (^:once fn* [] ~@body)))))

(s/fdef define-before-select
  :args (s/cat :dispatch-value some?
               :bindings       (s/spec (s/cat :args :clojure.core.specs.alpha/binding-form))
               :body           (s/+ any?))
  :ret any?)
