(ns toucan.util.test
  "Utility functions for writing tests with Toucan models."
  (:require
   [clojure.spec.alpha :as s]
   [toucan2.tools.with-temp :as with-temp]))

(s/def ::binding+properties
  (s/spec (s/cat :binding    :clojure.core.specs.alpha/binding-form
                 :properties (s/? any?))))

(defmacro with-temp [modelable [instance-binding properties] & body]
  `(with-temp/with-temp ~[modelable instance-binding properties]
     ~@body))

(s/fdef with-temp
  :args (s/cat :model              some?
               :binding+properties ::binding+properties
               :body               (s/+ any?))
  :ret  any?)

(defmacro with-temp* [model-bindings & body]
  `(with-temp/with-temp ~(vec (mapcat
                               (fn [[modelable [instance-binding properties]]]
                                 [modelable instance-binding properties])
                               (partition-all 2 model-bindings)))
     ~@body))

(s/fdef with-temp*
  :args (s/cat :bindings (s/spec (s/+ (s/cat :model              some?
                                             :binding+properties ::binding+properties)))
               :body     (s/+ any?))
  :ret  any?)

(defn with-temp-defaults
  [model]
  (with-temp/with-temp-defaults model))
