(ns toucan2.tools.before-select
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.util :as u]))

(set! *warn-on-reflection* true)

(defn ^:no-doc do-before-select
  "Impl for [[define-before-select]]. Don't call this directly."
  [model thunk]
  (u/try-with-error-context ["before select" {::model model}]
    (u/with-debug-result ["%s %s" `define-before-select model]
      (thunk))))

(defmacro define-before-select
  {:style/indent :defn, :arglists '([model [args-binding] & body]
                                    [[model query-class] [args-binding] & body])}
  [dispatch-value [args-binding] & body]
  (let [[model query-class] (if (vector? dispatch-value)
                              dispatch-value
                              [dispatch-value clojure.lang.IPersistentMap])]
    `(m/defmethod query/build :before [::select/select ~model ~query-class]
       [~'&query-type ~'&model ~args-binding]
       (do-before-select ~'&model (^:once fn* [] ~@body)))))

(s/fdef define-before-select
  :args (s/cat :dispatch-value (s/alt :model             some?
                                      :model+query-class (s/spec (s/cat :model       some?
                                                                        :query-class some?)))
               :bindings       (s/spec (s/cat :args :clojure.core.specs.alpha/binding-form))
               :body           (s/+ any?))
  :ret any?)
