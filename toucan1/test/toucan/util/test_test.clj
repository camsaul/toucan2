(ns toucan.util.test-test
  (:require
   [clojure.test :refer :all]
   [toucan.util.test :as t1.u.test]))

(deftest valid-syntax-test
  (are [form] (seqable? (macroexpand-1 form))
    `(t1.u.test/with-temp model [~'_] :ok)
    `(t1.u.test/with-temp model [~'instance {}] :ok)
    `(t1.u.test/with-temp model [{:keys [~'a]}] :ok)
    `(t1.u.test/with-temp model [{:keys [~'a]} {}] :ok)
    `(t1.u.test/with-temp* [model [~'_]] :ok)
    `(t1.u.test/with-temp* [model [~'instance {}]] :ok)
    `(t1.u.test/with-temp* [model [{:keys [~'a]}]] :ok)
    `(t1.u.test/with-temp* [model [{:keys [~'a]} {}]] :ok)
    `(t1.u.test/with-temp* [model [~'instance {}] model [~'instance]] :ok)
    `(t1.u.test/with-temp* [model [~'instance {}] model [~'_]] :ok)
    `(t1.u.test/with-temp* [model [~'instance {}] model [~'instance]] :ok)))
