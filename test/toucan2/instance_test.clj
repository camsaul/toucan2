(ns toucan2.instance-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.instance :as instance]
   [toucan2.protocols :as protocols]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.util :as u])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(deftest instance-test
  (let [m (assoc (instance/instance :wow {:a 100}) :b 200)]
    (is (= {:a 100, :b 200}
           m))
    (testing "original/changes"
      (is (= {:a 100}
             (protocols/original m)))
      (is (= {:b 200}
             (protocols/changes m)))
      (is (= {:a 300, :b 200}
             (protocols/changes (assoc m :a 300)))))
    (testing "with-original"
      (let [m2 (protocols/with-original m {:a 200, :b 200})]
        (is (= {:a 100, :b 200}
               m2))
        (is (= {:a 200, :b 200}
               (protocols/original m2)))
        (is (= {:a 100}
               (protocols/changes m2)))))
    (testing "table/with-table"
      (is (= :wow
             (protocols/model m)))
      (is (= :ok
             (protocols/model (protocols/with-model m :ok)))))
    (testing "current"
      (is (= {:a 100, :b 200}
             (protocols/current m)))
      (is (not (instance/instance? (protocols/current m)))))))

(deftest no-changes-return-this-test
  (testing "Operations that result in no changes to the underlying map should return instance as-is, rather than a new copy."
    ;; this is mostly important because we do optimizations in cases where `original` and `current` are identical objects.
    (let [m (instance/instance :bird {:a 100})]
      (testing "assoc"
        (let [m2 (assoc m :a 100)]
          (is (identical? m m2))
          (is (identical? (protocols/current m2)
                          (protocols/original m2)))))
      (testing "dissoc"
        (let [m2 (dissoc m :b)]
          (is (identical? m m2))
          (is (identical? (protocols/current m2)
                          (protocols/original m2)))))
      (testing "with-meta"
        (let [m2 (with-meta m (meta m))]
          (is (identical? m m2))))
      (testing "with-original"
        (let [m2 (protocols/with-original m (protocols/original m))]
          (is (identical? m m2))))
      (testing "with-current"
        (let [m2 (protocols/with-current m (protocols/current m))]
          (is (identical? m m2))))
      (testing "with-model"
        (let [m2 (protocols/with-model m (protocols/model m))]
          (is (identical? m m2)))))))

(deftest changes-test
  (is (= {:name "Hi-Dive"}
         (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
             (assoc :name "Hi-Dive")
             protocols/changes)))
  (testing "If there are no changes, `changes` should return `nil` for convenience."
    (is (= nil
           (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
               (assoc :name "Tempest")
               protocols/changes)))))

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
         (protocols/model (instance/instance ::MyModel))))
  (is (= ::MyModel
         (protocols/model (instance/instance ::MyModel {}))))
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
             (protocols/original (assoc m :original? false))))
      (is (= {}
             (dissoc m :original?)))
      (is (= {:original? true}
             (protocols/original (dissoc m :original?))))
      (is (= nil
             (protocols/original {})))
      (is (= nil
             (protocols/original nil))))))

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

(m/defmethod instance/key-transform-fn ::venues.no-key-transform
  [_model]
  identity)

(derive ::venues.no-key-transform ::test/venues)

(deftest no-key-xform-test
  (is (= (instance/instance ::test/venues {:id 1, :created-at (LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::test/venues {:select [:id :created-at]})))
  (testing "Should be able to disable key transforms by overriding `key-transform-fn*`"
    (let [[id created-at] (case (test/current-db-type)
                            :h2       [:ID :CREATED_AT]
                            :postgres [:id :created_at])]
      (is (= {id 1, created-at (LocalDateTime/parse "2017-01-01T00:00")}
             (select/select-one ::venues.no-key-transform {:select [:id :created-at]}))))))

(deftest pretty-print-test
  (testing "Should pretty-print"
    (testing "Normal output"
      (is (= (pr-str '(toucan2.instance/instance :venues {:id 1}))
             (pr-str (instance/instance :venues {:id 1})))))
    (testing "output with debugging enabled"
      (binding [u/*debug* true]
        (is (= (pr-str (list
                        'toucan2.instance/instance
                        :venues
                        (list
                         'toucan2.magic-map/magic-map
                         {:id 1}
                         (symbol "#'toucan2.magic-map/kebab-case-xform"))))
               (pr-str (instance/instance :venues {:id 1}))))))))

(deftest reset-original-test
  (let [m  (assoc (instance/instance :wow {:a 100}) :b 200)
        m2 (instance/reset-original m)]
    (is (= {:a 100, :b 200}
           m2))
    (is (= {:a 100, :b 200}
           (protocols/original m2)))
    (is (= nil
           (protocols/changes m2))))
  (testing "No-op for non-instances"
    (let [result (instance/reset-original {:a 1})]
      (is (= {:a 1}
             result))
      (is (= nil
             (protocols/original result)))
      (is (not (instance/instance? result))))))

(deftest update-original-test
  (let [m (-> (instance/instance :x :a 1)
              (assoc :b 2)
              (instance/update-original assoc :c 3))]
    (is (= {:a 1, :c 3}
           (protocols/original m)))
    (is (= (instance/instance :x {:a 1, :b 2})
           m))
    ;; A key being present in original but not in 'current' does not constitute a change
    (is (= {:b 2}
           (protocols/changes m)))
    (testing "No-op for non-instances"
      (let [result (instance/update-original {:a 1} assoc :c 3)]
        (is (= {:a 1}
               result))
        (is (= nil
               (protocols/original result)))
        (is (not (instance/instance? result)))))))

(deftest update-original-and-current-test
  (let [m (-> (instance/instance :x :a 1)
              (assoc :b 2)
              (instance/update-original-and-current assoc :c 3))]
    (is (= {:a 1, :c 3}
           (protocols/original m)))
    (is (= (instance/instance :x {:a 1, :b 2, :c 3})
           m))
    ;; A key being present in original but not in 'current' does not constitute a change
    (is (= {:b 2}
           (protocols/changes m)))
    (testing "Just act like regular 'apply' for non-instances"
      (let [result (instance/update-original-and-current {:a 1} assoc :c 3)]
        (is (= {:a 1, :c 3}
               result))
        (is (= nil
               (protocols/original result)))
        (is (not (instance/instance? result)))))
    (testing "If original and current were previously identical, they should be after update as well."
      (let [m (instance/instance :x {:a 1})]
        (is (identical? (protocols/original m)
                        (protocols/current m)))
        (let [m2 (instance/update-original-and-current m assoc :b 2)]
          (is (= (protocols/original m2)
                 (protocols/current m2)))
          (is (identical? (protocols/original m2)
                          (protocols/current m2))))))))

(deftest dispatch-value-test
  (testing "Instance should implement dispatch-value"
    (is (= :wow
           (protocols/dispatch-value (instance/instance :wow {}))))))

(derive ::toucan ::bird)

(deftest instance-of?-test
  (is (instance/instance? (instance/instance ::toucan {})))
  (is (instance/instance-of? ::bird (instance/instance ::toucan {})))
  (are [x] (not (instance/instance-of? ::toucan x))
    (instance/instance ::bird {})
    nil
    {}
    (instance/instance ::shoe {})))

(deftest no-nil-maps-test
  (testing "Shouldn't be able to make the maps in an instance nil"
    (let [m1 (instance/instance ::birbs {:a 1})]
      (is (= {:a 1}
             (protocols/original m1)
             (protocols/current m1)))
      (testing "using"
        (testing "with-original"
          (let [m2 (protocols/with-original m1 nil)]
            (is (= {}
                   (protocols/original m2)))))
        (testing "with-current"
          (let [m2 (protocols/with-current m1 nil)]
            (is (= {}
                   (protocols/current m2)))))))))

(deftest validate-new-maps-test
  (testing "Shouldn't be able to make the maps something invalid"
    (let [m1 (instance/instance ::birbs {:a 1})]
      (is (= {:a 1}
             (protocols/original m1)
             (protocols/current m1)))
      (testing "using"
        (testing "with-original"
          (is (thrown-with-msg?
               Throwable
               #"Assert failed: \(map\? new-original\)"
               (protocols/with-original m1 1))))
        (testing "with-current"
          (is (thrown-with-msg?
               Throwable
               #"Assert failed: \(map\? new-current\)"
               (protocols/with-current m1 1))))))))
