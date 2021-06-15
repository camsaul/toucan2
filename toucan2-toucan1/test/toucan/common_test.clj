(ns toucan.common-test
  (:require [clojure.test :refer :all]
            [toucan.common :as common]
            [toucan.test-models.user :refer [User]]))

(deftest resolve-model-test
  (is (= :models/User
         (common/resolve-model 'User)))
  (testing "If model is already resolved it should just return it as-is"
    (is (= User
           (common/resolve-model User))))
  (testing "Throw Exception when trying to resolve things that aren't entities or symbols"
    (is (thrown?
         Exception
         (common/resolve-model {}))))
  (is (thrown?
       Exception
       (common/resolve-model 100)))
  (is (thrown?
       Exception
       (common/resolve-model "User")))
  (is (thrown?
       Exception
       (common/resolve-model :user)))
  (testing "entities symbols are NO LONGER case-sensitive"
    (is (= :models/user
           (common/resolve-model 'user)))))
