(ns bluejdbc.options-test
  (:require [bluejdbc.options :as options]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(deftest map->Properties-test
  (testing "converting a Clojure map to Properties"
    (testing "keys and values should be converted to strings"
      ;; Properties are equality-comparable to maps
      (is (= {"A" "true", "B" "false", "C" "100"}
             (options/->Properties {:A "true", "B" false, "C" 100}))))))

(defrecord ^:private MockOptions [opts changes]
  options/Options
  (options [_] opts)
  (with-options* [_ new-options] (MockOptions. new-options changes)))

(m/defmethod options/set-options! [MockOptions :default]
  [this new-options]
  (reset! (:changes this) new-options))

(deftest with-options-test
  (let [x  (with-meta (MockOptions. {:a 1, :b 2} (atom nil)) {:cool true})
        x' (options/with-options x {:b 3, :c 4})]
    (testing "Sanity check"
      (is (= {:a 1, :b 2}
             (options/options x))))

    (testing "with-options should merge existing options"
      (is (= {:a 1, :b 3, :c 4}
             (options/options x'))))

    (testing "with-options should preserve metadata"
      (is (= {:cool true}
             (meta x'))))

    (testing "with-options should call set-options!, but only for changes"
      (is (= {:b 3, :c 4}
             @(:changes x'))))

    (testing "with-options should no-op if options have no changes"
      (is (identical? x
                      (options/with-options x {:a 1, :b 2}))))))
