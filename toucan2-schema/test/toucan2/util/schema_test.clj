(ns toucan2.util.schema-test
  (:require [clojure.test :refer :all]
            [schema.core :as s]
            [toucan2.instance :as instance]
            [toucan2.util.schema :as t2.schema]))

(def User (t2.schema/instance-of :models/User))

(deftest instance-of-test
  (is (s/check User {}))
  (is (s/check User nil))
  (is (= (str "(named (not (instance? toucan2.instance.IInstance nil))"
              " \"Toucan 2 instance of :models/User\")")
         (pr-str (s/check User nil))))
  (is (nil? (s/check User (instance/instance :models/User {})))))
