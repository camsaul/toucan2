(ns toucan2.tools.simple-out-transform
  (:require
   [clojure.spec.alpha :as s]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.pipeline :as pipeline]))

;; TODO -- I'm not really convinced this is worth it at all. It's used in exactly one place =(

(defn -xform [f]
  (map (fn [instance]
         (let [instance (f instance)]
           (cond-> instance
             (instance/instance? instance)
             instance/reset-original)))))

(defmacro define-out-transform
  {:style/indent :defn}
  [[query-type model-type] [instance-binding] & body]
  `(m/defmethod pipeline/results-transform [~query-type ~model-type]
     [~'&query-type ~'&model]
     (let [xform# (-xform (fn [~instance-binding] ~@body))]
       (comp xform#
             (~'next-method ~'&query-type ~'&model)))))

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
