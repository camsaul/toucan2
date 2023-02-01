(ns toucan2.util-test
  (:require
   [clojure.test :refer :all]
   [toucan2.util :as u]))

(deftest ^:parallel ->kebab-case-test
  (are [k expected] (= expected
                       (u/->kebab-case k))
    "id"     "id"
    "id_1"   "id-1"
    :id      :id
    :id_1    :id-1
    :id/id_1 :id/id-1))
