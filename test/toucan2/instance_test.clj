(ns toucan2.instance-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.util :as u])
  (:import
   (java.util Locale)))

(deftest default-key-transform-turkish-test
  (testing "Test that identifiers are correctly lower cased in Turkish locale"
    (let [original-locale (Locale/getDefault)]
      (try
        (Locale/setDefault (Locale/forLanguageTag "tr"))
        ;; if we used `clojure.string/lower-case`, `:ID` would be converted to `:ıd` in Turkish locale
        (is (= :id
               (instance/default-key-transform "ID")
               (instance/default-key-transform :ID)))
        (finally
          (Locale/setDefault original-locale))))))

(deftest instance-test
  (let [m (assoc (instance/instance :wow {:a 100}) :b 200)]
    (is (= {:a 100, :b 200}
           m))
    (testing "original/changes"
      (is (= {:a 100}
             (instance/original m)))
      (is (= {:b 200}
             (instance/changes m)))
      (is (= {:a 300, :b 200}
             (instance/changes (assoc m :a 300)))))
    (testing "with-original"
      (let [m2 (instance/with-original m {:a 200, :b 200})]
        (is (= {:a 100, :b 200}
               m2))
        (is (= {:a 200, :b 200}
               (instance/original m2)))
        (is (= {:a 100}
               (instance/changes m2)))))
    (testing "table/with-table"
      (is (= :wow
             (instance/model m)))
      (is (= :ok
             (instance/model (instance/with-model m :ok)))))
    (testing "current"
      (is (= {:a 100, :b 200}
             (instance/current m)))
      (is (not (instance/instance? (instance/current m)))))))

(deftest no-changes-return-this-test
  (testing "Operations that result in no changes to the underlying map should return instance as-is, rather than a new copy."
    ;; this is mostly important because we do optimizations in cases where `original` and `current` are identical objects.
    (let [m (instance/instance :bird {:a 100})]
      (testing "assoc"
        (let [m2 (assoc m :a 100)]
          (is (identical? m m2))
          (is (identical? (instance/current m2)
                          (instance/original m2)))))
      (testing "dissoc"
        (let [m2 (dissoc m :b)]
          (is (identical? m m2))
          (is (identical? (instance/current m2)
                          (instance/original m2)))))
      (testing "with-meta"
        (let [m2 (with-meta m (meta m))]
          (is (identical? m m2))))
      (testing "with-original"
        (let [m2 (instance/with-original m (instance/original m))]
          (is (identical? m m2))))
      (testing "with-current"
        (let [m2 (instance/with-current m (instance/current m))]
          (is (identical? m m2))))
      (testing "with-model"
        (let [m2 (instance/with-model m (instance/model m))]
          (is (identical? m m2)))))))

(deftest changes-test
  (is (= {:name "Hi-Dive"}
         (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
             (assoc :name "Hi-Dive")
             instance/changes)))
  (testing "If there are no changes, `changes` should return `nil` for convenience."
    (is (= nil
           (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
               (assoc :name "Tempest")
               instance/changes)))))

(deftest contains-key-test
  (is (= true
         (.containsKey (instance/instance :wow {:some-key true}) :some-key)))
  (testing "unnormalized keys"
    (is (= true
           (.containsKey (instance/instance :wow {:some-key true}) :SOME_KEY)))))

(deftest equality-test
  (testing "equality"
    (testing "two instances with the same Table should be equal"
      (is (= (instance/instance :wow {:a 100})
             (instance/instance :wow {:a 100}))))
    (testing "Two instances with different Tables are not equal"
      (is (not= (instance/instance :other {:a 100})
                (instance/instance :wow {:a 100}))))
    (testing "An Instance should be considered equal to a plain map for convenience purposes"
      (testing "map == instance"
        (is (= {:a 100}
               (instance/instance :wow {:a 100}))))
      (testing "instance == map"
        (is (= (instance/instance :wow {:a 100})
               {:a 100}))))))

(deftest instance-test-2
  (is (= {}
         (instance/instance ::MyModel)))
  (is (= {}
         (instance/instance ::MyModel {})))
  (is (= {:a 1}
         (instance/instance ::MyModel {:a 1})))
  (is (= ::MyModel
         (instance/model (instance/instance ::MyModel))))
  (is (= ::MyModel
         (instance/model (instance/instance ::MyModel {}))))
  (let [m (instance/instance ::MyModel {:original? true})]
    (is (= {:original? false}
           (assoc m :original? false)))
    (is (= {:original? true}
           (.orig m)))
    (is (= {:original? true}
           (.m m)))
    (is (= {:original? true}
           (.orig ^toucan2.instance.Instance (assoc m :original? false))))
    (is (= {:original? false}
           (.m ^toucan2.instance.Instance (assoc m :original? false))))
    (testing "fetching original value"
      (is (= {:original? true}
             (instance/original (assoc m :original? false))))
      (is (= {}
             (dissoc m :original?)))
      (is (= {:original? true}
             (instance/original (dissoc m :original?))))
      (is (= nil
             (instance/original {})))
      (is (= nil
             (instance/original nil))))))

(deftest transient-test
  (let [m  (transient (instance/instance :wow {:a 100}))
        m' (-> m
               (assoc! :b 200)
               (assoc! :c 300)
               (dissoc! :a)
               persistent!)]
    (is (= {:b 200, :c 300}
           m'))))

;;;; Magic Map stuff.

(deftest default-key-transform-test
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
             (instance/default-key-transform k))))))

(deftest normalize-map-test
  (testing "Should normalize keys"
    (is (= {:abc 100, :d-ef 200}
           (instance/normalize-map instance/default-key-transform {:ABC 100, "dEf" 200}))))
  (testing "Should preserve metadata"
    (is (= true
           (:wow (meta (instance/normalize-map instance/default-key-transform (with-meta {} {:wow true}))))))))

(deftest magic-create-test
  (let [m (instance/instance nil {:snake/snake_case 1, "SCREAMING_SNAKE_CASE" 2, :lisp-case 3, :ANGRY/LISP-CASE 4})]
    (is (= (instance/instance nil {:snake/snake-case 1, "screaming-snake-case" 2, :lisp-case 3, :angry/lisp-case 4})
           m)))
  (testing "Should be able to create a map with varargs"
    (is (= {:db-id 1}
           (instance/instance nil :db_id 1)))
    (is (= {:db-id 1, :table-id 2}
           (instance/instance nil :db_id 1, :TABLE_ID 2))))
  (testing "Should preserve metadata"
    (is (= {:x 100}
           #_{:x 100, :type :x}         ; TODO -- not sure Toucan should set `:type` metadata
           (meta (instance/instance :x (with-meta {} {:x 100})))))))

(deftest magic-keys-test
  (testing "keys"
    (let [m (instance/instance nil {:db_id 1, :table_id 2})]
      (testing "get keys"
        (testing "get"
          (is (= 1
                 (:db_id m)))
          (is (= 1
                 (:db-id m))))
        (is (= [:db-id :table-id]
               (keys m)))
        (testing "assoc"
          (is (= (instance/instance nil {:db-id 2, :table-id 2})
                 (assoc m :db_id 2)))
          (is (= (instance/instance nil {:db-id 3, :table-id 2})
                 (assoc m :db-id 3))))
        (testing "dissoc"
          (is (= {}
                 (dissoc (instance/instance nil "ScReAmInG_SnAkE_cAsE" 1) "sc-re-am-in-g-sn-ak-e-c-as-e"))))
        (testing "update"
          (is (= (instance/instance nil {:db-id 2, :table-id 2})
                 (update m :db_id inc))))))))

(deftest magic-equality-test
  (testing "Two maps created with different key cases should be equal"
    (is (= (instance/instance nil {:db-id 1, :table-id 2})
           (instance/instance nil {:db_id 1, :table_id 2}))))
  (testing "should be equal to normal map with the same keys"
    (testing "map == instance"
      (is (= {:db-id 1, :table-id 2}
             (instance/instance nil {:db_id 1, :table_id 2}))))
    (testing "instance == map"
      (is (= (instance/instance nil {:db_id 1, :table_id 2})
             {:db-id 1, :table-id 2})))
    (is (= {}
           (instance/instance nil {})))))

(m/defmethod instance/key-transform-fn :toucan2.instance-test.no-key-transform/venues
  [_model]
  identity)

(derive :toucan2.instance-test.no-key-transform/venues ::test/venues)

(deftest no-key-xform-test
  (is (= (instance/instance ::test/venues {:id 1, :created-at (java.time.LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::test/venues {:select [:id :created-at]})))
  (testing "Should be able to disable key transforms by overriding `key-transform-fn*`"
    (is (= {:id 1, :created_at (java.time.LocalDateTime/parse "2017-01-01T00:00")}
           (select/select-one :toucan2.instance-test.no-key-transform/venues {:select [:id :created-at]})))))

(deftest pretty-print-test
  (testing "Should pretty-print"
    (is (= "(toucan2.instance/instance :venues {:id 1})"
           (pr-str (instance/instance :venues {:id 1}))))))

(deftest reset-original-test
  (let [m  (assoc (instance/instance :wow {:a 100}) :b 200)
        m2 (instance/reset-original m)]
    (is (= {:a 100, :b 200}
           m2))
    (is (= {:a 100, :b 200}
           (instance/original m2)))
    (is (= nil
           (instance/changes m2))))
  (testing "No-op for non-instances"
    (let [result (instance/reset-original {:a 1})]
      (is (= {:a 1}
             result))
      (is (= nil
             (instance/original result)))
      (is (not (instance/instance? result))))))

(deftest update-original-test
  (let [m (-> (instance/instance :x :a 1)
              (assoc :b 2)
              (instance/update-original assoc :c 3))]
    (is (= {:a 1, :c 3}
           (instance/original m)))
    (is (= (instance/instance :x {:a 1, :b 2})
           m))
    ;; A key being present in original but not in 'current' does not constitute a change
    (is (= {:b 2}
           (instance/changes m)))
    (testing "No-op for non-instances"
      (let [result (instance/update-original {:a 1} assoc :c 3)]
        (is (= {:a 1}
               result))
        (is (= nil
               (instance/original result)))
        (is (not (instance/instance? result)))))))

(deftest update-original-and-current-test
  (let [m (-> (instance/instance :x :a 1)
              (assoc :b 2)
              (instance/update-original-and-current assoc :c 3))]
    (is (= {:a 1, :c 3}
           (instance/original m)))
    (is (= (instance/instance :x {:a 1, :b 2, :c 3})
           m))
    ;; A key being present in original but not in 'current' does not constitute a change
    (is (= {:b 2}
           (instance/changes m)))
    (testing "Just act like regular 'apply' for non-instances"
      (let [result (instance/update-original-and-current {:a 1} assoc :c 3)]
        (is (= {:a 1, :c 3}
               result))
        (is (= nil
               (instance/original result)))
        (is (not (instance/instance? result)))))
    (testing "If original and current were previously identical, they should be after update as well."
      (let [m (instance/instance :x {:a 1})]
        (is (identical? (instance/original m)
                        (instance/current m)))
        (let [m2 (instance/update-original-and-current m assoc :b 2)]
          (is (= (instance/original m2)
                 (instance/current m2)))
          (is (identical? (instance/original m2)
                          (instance/current m2))))))))

(deftest dispatch-value-test
  (testing "Instance should implement dispatch-value"
    (is (= :wow
           (u/dispatch-value (instance/instance :wow {}))))))

(derive ::toucan ::bird)

(deftest instance-of?-test
  (is (instance/instance-of? (instance/instance ::toucan {}) ::bird))
  (is (not (instance/instance-of? (instance/instance ::bird {}) ::toucan)))
  (is (not (instance/instance-of? nil ::toucan)))
  (is (not (instance/instance-of? {} ::toucan)))
  (is (not (instance/instance-of? (instance/instance ::shoe {}) ::toucan))))

#_(deftest type-metadata-test
  (testing "Instances should get ^:type metadata when you create them, so you can dispatch with `type`."
    (let [model (u/dispatch-on "my_table" ::my-type)]
      (doseq [instance [(instance/instance model)
                        (instance/instance model {})
                        (instance/instance model {})
                        (instance/instance model :x :y)]]
        (is (= ::my-type
               (:type (meta instance))
               (type instance))))))
  (testing "e2e test: make sure instances from something like select come back with ^:type metadata"
    (let [instance (select/select-one [:test/postgres :venues] 1)]
      (is (some? instance))
      (is (= :venues
             (:type (meta instance))
             (type instance))))))
