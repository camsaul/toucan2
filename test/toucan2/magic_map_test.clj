(ns toucan2.magic-map-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.magic-map :as magic-map])
  (:import
   (java.util Locale)))

(deftest kebab-case-xform-turkish-test
  (testing "Test that identifiers are correctly lower cased in Turkish locale"
    (let [original-locale (Locale/getDefault)]
      (try
        (Locale/setDefault (Locale/forLanguageTag "tr"))
        ;; if we used `clojure.string/lower-case`, `:ID` would be converted to `:ıd` in Turkish locale
        (is (= :id
               (magic-map/kebab-case-xform "ID")
               (magic-map/kebab-case-xform :ID)))
        (finally
          (Locale/setDefault original-locale))))))

(deftest contains-key-test
  (is (= true
         (.containsKey (magic-map/magic-map {:some-key true}) :some-key)))
  (testing "unnormalized keys"
    (is (= true
           (.containsKey (magic-map/magic-map {:some-key true}) :SOME_KEY)))))

(deftest kebab-case-xform-test
  (doseq [k-str  ["my-key" "my_key"]
          k-str  [k-str (str/upper-case k-str)]
          ns-str [nil "my-ns" "my_ns"]
          ns-str (if ns-str
                   [ns-str (str/upper-case ns-str)]
                   [ns-str])
          k      [k-str
                  (keyword ns-str k-str)]
          :let   [expected (cond
                             (string? k) :my-key
                             ns-str      :my-ns/my-key
                             :else       :my-key)]]
    (testing (format "%s -> %s" (pr-str k) (pr-str expected))
      (is (= expected
             (magic-map/kebab-case-xform k))))))

(deftest kebab-case-xform-test-2
  (are [k expected] (= expected
                       (magic-map/kebab-case-xform k))
    :my_col              :my-col
    :MyCol               :my-col
    :myCol               :my-col
    :my-col              :my-col
    :my_namespace/my_col :my-namespace/my-col
    "my_col"             :my-col
    'my_namespace/my_col :my-namespace/my-col))

(deftest normalize-map-test
  (testing "Should normalize keys"
    (is (= {:abc 100, :d-ef 200}
           (magic-map/normalize-map magic-map/kebab-case-xform {:ABC 100, "dEf" 200}))))
  (testing "Should preserve metadata"
    (is (= true
           (:wow (meta (magic-map/normalize-map magic-map/kebab-case-xform (with-meta {} {:wow true}))))))))

(deftest magic-create-test
  (let [m (magic-map/magic-map {:snake/snake_case 1, "SCREAMING_SNAKE_CASE" 2, :lisp-case 3, :ANGRY/LISP-CASE 4})]
    (is (= (magic-map/magic-map {:snake/snake-case 1, "screaming-snake-case" 2, :lisp-case 3, :angry/lisp-case 4})
           m)))
  (testing "Should preserve metadata"
    (is (= {:x 100}
           (meta (magic-map/magic-map (with-meta {} {:x 100})))))))

(deftest magic-keys-test
  (testing "keys"
    (let [m (magic-map/magic-map {:db_id 1, :table_id 2})]
      (testing "get keys"
        (testing "get"
          (is (= 1
                 (:db_id m)))
          (is (= 1
                 (:db-id m))))
        (is (= [:db-id :table-id]
               (keys m)))
        (testing "assoc"
          (is (= (magic-map/magic-map {:db-id 2, :table-id 2})
                 (assoc m :db_id 2)))
          (is (= (magic-map/magic-map {:db-id 3, :table-id 2})
                 (assoc m :db-id 3))))
        (testing "dissoc"
          (is (= {}
                 (dissoc (magic-map/magic-map {"ScReAmInG_SnAkE_cAsE" 1}) "sc-re-am-in-g-sn-ak-e-c-as-e"))))
        (testing "update"
          (is (= (magic-map/magic-map {:db-id 2, :table-id 2})
                 (update m :db_id inc))))))))

(deftest magic-equality-test
  (testing "Two maps created with different key cases should be equal"
    (is (= (magic-map/magic-map {:db-id 1, :table-id 2})
           (magic-map/magic-map {:db_id 1, :table_id 2}))))
  (testing "should be equal to normal map with the same keys"
    (testing "map == instance"
      (is (= {:db-id 1, :table-id 2}
             (magic-map/magic-map {:db_id 1, :table_id 2}))))
    (testing "instance == map"
      (is (= (magic-map/magic-map {:db_id 1, :table_id 2})
             {:db-id 1, :table-id 2})))
    (is (= {}
           (magic-map/magic-map {})))))

(deftest no-key-xform-test
  (testing "Should be able to disable key transforms by passing in a different key transform function"
    (is (= {:id 1, :created-at 2}
           (magic-map/magic-map {:id 1, :created_at 2})))
    (is (= {:id 1, :created_at 2}
           (magic-map/magic-map {:id 1, :created_at 2} identity)))))

(deftest pretty-print-test
  (testing "Should pretty-print"
    (is (= "(toucan2.map-types.magic-map/magic-map {:id 1} #'toucan2.map-types.magic-map/kebab-case-xform)"
           (pr-str (magic-map/magic-map {:id 1}))))))
