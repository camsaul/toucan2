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

(deftest ^:synchronized unparent-descendants-test
  (with-redefs [clojure.core/global-hierarchy (make-hierarchy)]
  (testing "unparent-descendants removes parent from all descendants"
    (testing "basic case - single descendant"
      (let [ancestor ::test-ancestor-1
            descendant ::test-descendant-1
            parent-to-remove ::test-parent-1]
        (derive descendant ancestor)
        (derive descendant parent-to-remove)
        (testing "sanity"
          (is (isa? descendant ancestor))
          (is (isa? descendant parent-to-remove)))
        (u/unparent-descendants ancestor parent-to-remove)
        (is (isa? descendant ancestor))
        (is (not (isa? descendant parent-to-remove)))
        (underive descendant parent-to-remove)
        (underive descendant ancestor)))
    (testing "multiple descendants"
      (let [ancestor ::test-ancestor-2
            descendant-1 ::test-descendant-2a
            descendant-2 ::test-descendant-2b
            parent-to-remove ::test-parent-2]
        (derive descendant-1 ancestor)
        (derive descendant-1 parent-to-remove)
        (derive descendant-2 ancestor)
        (derive descendant-2 parent-to-remove)
        (u/unparent-descendants ancestor parent-to-remove)
        (is (not (isa? descendant-1 parent-to-remove)))
        (is (not (isa? descendant-2 parent-to-remove)))
        (underive descendant-1 ancestor)
        (underive descendant-2 ancestor)))
    (testing "no descendants"
      (let [ancestor ::test-ancestor-3
            parent-to-remove ::test-parent-3]
        (testing "sanity"
          (is (empty? (descendants ancestor))))
        (u/unparent-descendants ancestor parent-to-remove)))
    (testing "descendant not derived from parent-to-remove"
      (let [ancestor ::test-ancestor-4
            descendant ::test-descendant-4
            parent-to-remove ::test-parent-4
            other-parent ::test-other-parent-4]
        (derive descendant ancestor)
        (derive descendant other-parent)
        (testing "sanity"
          (is (isa? descendant ancestor))
          (is (isa? descendant other-parent))
          (is (not (isa? descendant parent-to-remove))))
        (u/unparent-descendants ancestor parent-to-remove)
        (is (isa? descendant ancestor))
        (is (isa? descendant other-parent))
        (underive descendant ancestor)
          (underive descendant other-parent))))))
