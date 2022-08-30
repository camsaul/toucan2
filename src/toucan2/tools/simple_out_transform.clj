(ns toucan2.tools.simple-out-transform
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]))

(defmacro define-out-transform
  {:style/indent :defn}
  [[query-type model-type] [instance-binding] & body]
  `(m/defmethod pipeline/transduce-with-model :around [~query-type ~model-type]
     [rf# ~'&query-type ~'&model ~'&parsed-args]
     (let [rf*# ((map (fn [~instance-binding]
                        (let [instance# (do ~@body)]
                          (cond-> instance#
                            (instance/instance? instance#)
                            instance/reset-original))))
                 rf#)]
       (~'next-method rf*# ~'&query-type ~'&model ~'&parsed-args))))

(s/fdef define-out-transform
  :args (s/cat :dispatch-value (s/spec (s/cat :query-type keyword?
                                              :model-type any?))
               :bindings (s/spec (s/cat :row :clojure.core.specs.alpha/binding-form))
               :body     (s/+ any?))
  :ret any?)

(comment
  (define-out-transform [:toucan.query-type/select.instances ::venues]
    [row]
    (assoc row :neat true)))
