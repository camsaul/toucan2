(ns toucan2.instance-test
  (:require
   [clojure.test :refer :all]
   [toucan2.instance :as instance]
   [toucan2.protocols :as protocols]))

(set! *warn-on-reflection* true)

(deftest ^:parallel instance-test
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

(deftest ^:parallel no-changes-return-this-test
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

(deftest ^:parallel changes-test
  (is (= {:name "Hi-Dive"}
         (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
             (assoc :name "Hi-Dive")
             protocols/changes)))
  (testing "If there are no changes, `changes` should return `nil` for convenience."
    (is (= nil
           (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
               (assoc :name "Tempest")
               protocols/changes))))
  (testing "Ignore columns that got removed"
    (let [m (-> (instance/instance :places {:id 1, :bird-info {:type :toucan, :num-cans 2, :good-bird? true}})
                (dissoc :good-bird?))]
      (is (= nil
             (protocols/changes m)))))
  (testing "Do not do a deep diff! Only diff the top-level"
    (testing 'assoc
      (let [m (-> (instance/instance :places {:id 1, :bird-info {:type :toucan, :num-cans 2, :good-bird? true}})
                  (assoc-in [:bird-info :favorite-snacc] :blueberries))]
        (is (= {:bird-info {:type :toucan, :num-cans 2, :good-bird? true, :favorite-snacc :blueberries}}
               (protocols/changes m)))))
    (testing 'update
      (let [m (-> (instance/instance :places {:id 1, :bird-info {:type :toucan, :num-cans 2, :good-bird? true}})
                  (assoc-in [:bird-info :num-cans] 4))]
        (is (= {:bird-info {:type :toucan, :num-cans 4, :good-bird? true}}
               (protocols/changes m)))))
    (testing 'dissoc
      (let [m (-> (instance/instance :places {:id 1, :bird-info {:type :toucan, :num-cans 2, :good-bird? true}})
                  (update :bird-info dissoc :good-bird?))]
        (is (= {:bird-info {:type :toucan, :num-cans 2}}
               (protocols/changes m)))))
    (testing "sequences"
      (testing "add an element"
        (let [m (-> (instance/instance :places {:id 1, :parakeets ["Parroty" "Green Friend" "Parrot Hilton"]})
                    (update :parakeets conj "Egg"))]
          (is (= {:parakeets ["Parroty" "Green Friend" "Parrot Hilton" "Egg"]}
                 (protocols/changes m)))))
      (testing "remove an element"
        (let [m (-> (instance/instance :places {:id 1, :parakeets ["Parroty" "Green Friend" "Parrot Hilton"]})
                    (update :parakeets rest))]
          (is (= {:parakeets ["Green Friend" "Parrot Hilton"]}
                 (protocols/changes m))))))
    (testing "type changed completely"
      (let [parrot-vector ["Parroty" "Green Friend" "Parrot Hilton"]
            parrot-count  4
            parrot-map    {:num-birbs 4}]
        (doseq [[before after] [[parrot-vector parrot-count]
                                [parrot-vector parrot-map]
                                [parrot-count parrot-vector]
                                [parrot-count parrot-map]
                                [parrot-map parrot-vector]
                                [parrot-map parrot-count]]]
          (testing (format "%s => %s" (pr-str before) (pr-str after))
            (let [m (-> (instance/instance :places {:id 1, :parakeets before})
                        (assoc :parakeets after))]
              (is (= {:parakeets after}
                     (protocols/changes m))))))))))

(deftest ^:parallel equality-test
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
               (instance/instance :wow {:a 100})))
        (is (= {}
               (instance/instance nil {}))))
      (testing "instance == map"
        (is (= (instance/instance :wow {:a 100})
               {:a 100}))))))

(deftest ^:parallel instance-test-2
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

(deftest ^:parallel transient-test
  (let [m  (transient (instance/instance :wow {:a 100}))
        m' (-> m
               (assoc! :b 200)
               (assoc! :c 300)
               (dissoc! :a)
               persistent!)]
    (is (= {:b 200, :c 300}
           m'))))

(deftest ^:parallel transient-val-at-test
  (let [^clojure.lang.ITransientMap m (transient (instance/instance :wow {:a 100}))
        m'                            (assoc! m :b 200)]
    (doseq [^clojure.lang.ITransientMap m [m m']]
      (is (= 100
             (.valAt m :a)))
      (is (= 200
             (.valAt m :b)))
      (is (= nil
             (.valAt m :c)))
      (is (= 100
             (.valAt m :a ::not-found)))
      (is (= 200
             (.valAt m :b ::not-found)))
      (is (= ::not-found
             (.valAt m :c ::not-found)))))
  (testing "namespaced keys"
    (let [^clojure.lang.ITransientMap m (transient (instance/instance))
          m'                            (assoc! m ::key [1])]
      (doseq [^clojure.lang.ITransientMap m [m m']]
        (is (= [1]
               (.valAt m ::key)))))))

(deftest ^:parallel transient-contains?-test
  (let [m (transient (instance/instance :wow {:a 100}))]
    (is (contains? m :a))
    (is (not (contains? m :b)))))

(deftest ^:parallel transient-count-test
  (let [^clojure.lang.ITransientMap m (transient (instance/instance :wow {:a 100}))]
    (is (= 1
           (count m)))
    (let [m' (assoc! m :b 200)]
      (doseq [^clojure.lang.ITransientMap m [m m']]
        (is (= 2
               (count m)))))))

(deftest ^:parallel create-test
  (testing "Should be able to create a map with varargs"
    (is (= {:db-id 1}
           (instance/instance nil :db-id 1)))
    (is (= {:db-id 1, :table-id 2}
           (instance/instance nil :db-id 1, :table-id 2))))
  (testing "Should preserve metadata"
    (is (= {:x 100}
           (meta (instance/instance :x (with-meta {} {:x 100})))))))

(deftest ^:parallel keys-test
  (let [m (instance/instance nil {:db-id 1, :table-id 2})]
    (testing "get keys"
      (testing "get"
        (is (= 1
               (:db-id m))))
      (is (= [:db-id :table-id]
             (keys m)))
      (testing "assoc"
        (is (= (instance/instance nil {:db-id 2, :table-id 2})
               (assoc m :db-id 2)))
        (is (= (instance/instance nil {:db-id 3, :table-id 2})
               (assoc m :db-id 3))))
      (testing "dissoc"
        (is (= {}
               (dissoc (instance/instance nil :db-id 1) :db-id))))
      (testing "update"
        (is (= (instance/instance nil {:db-id 2, :table-id 2})
               (update m :db-id inc)))))))

(deftest ^:parallel pretty-print-test
  (testing "Should pretty-print"
    (is (= (pr-str '(toucan2.instance/instance :venues {:id 1}))
           (pr-str (instance/instance :venues {:id 1}))))))

(deftest ^:parallel reset-original-test
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

(deftest ^:parallel update-original-test
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

(deftest ^:parallel update-original-and-current-test
  (let [m1 (-> (instance/instance :x :a 1)
               (assoc :b 2))]
    (are [f expected] (= (instance/instance :x expected)
                         (f m1))
      identity             {:a 1, :b 2}
      #'protocols/current  {:a 1, :b 2}
      #'protocols/original {:a 1}
      #'protocols/changes  {:b 2})
    (let [m2 (instance/update-original-and-current m1 assoc :c 3)]
      (are [f expected] (= (instance/instance :x expected)
                           (f m2))
        identity             {:a 1, :b 2, :c 3}
        #'protocols/current  {:a 1, :b 2, :c 3}
        #'protocols/original {:a 1, :c 3}
        ;; A key being present in original but not in 'current' does not constitute a change
        #'protocols/changes  {:b 2}))
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

(deftest ^:parallel dispatch-value-test
  (testing "Instance should implement dispatch-value"
    (is (= :wow
           (protocols/dispatch-value (instance/instance :wow {}))))))

(derive ::toucan ::bird)

(deftest ^:parallel instance-of?-test
  (is (instance/instance? (instance/instance ::toucan {})))
  (is (instance/instance-of? ::bird (instance/instance ::toucan {})))
  (are [x] (not (instance/instance-of? ::toucan x))
    (instance/instance ::bird {})
    nil
    {}
    (instance/instance ::shoe {})))

(deftest ^:parallel no-nil-maps-test
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

(deftest ^:parallel validate-new-maps-test
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

(deftest ^:parallel dont-create-new-map-if-model-is-same-test
  (let [m1 (instance/instance ::birbs {:a 1})
        m2 (instance/instance ::birbs m1)]
    (is (identical? m1 m2))))

(deftest ^:parallel lots-of-keys-test
  (testing "Make sure things still work when we pass the threshold from ArrayMap -> HashMap"
    (let [m {:date_joined   :%now
             :email         "nobody@nowhere.com"
             :first_name    "No"
             :is_active     true
             :is_superuser  false
             :last_login    nil
             :last_name     "Body"
             :password      "$2a$10$B/Cu7Nva.Ad.0gtu5g3o7udSbBl2r4YG.jRnxvIUWuNzmO7LCu2Cy"
             :password_salt "c4467d75-59d2-41a5-9499-e6c5d8db923b"
             :updated_at    :%now}]
      (testing `instance/instance
        (is (= m
               (instance/instance
                :models/User
                m))))
      (testing `into
        (is (= m
              (into (instance/instance :models/User) m)))))))

(deftest ^:parallel empty-test
  (testing "(empty instance) should return an empty instance of the same model"
    (let [instance (instance/instance ::venues {:id 1, :name "Tempest", :category "bar"})]
      (is (= {:id 1, :name "Tempest", :category "bar"}
             instance))
      (let [empty-instance (empty instance)]
        (is (= {}
               empty-instance))
        (is (instance/instance? empty-instance))
        (is (instance/instance-of? ::venues empty-instance))))))

(deftest ^:parallel merge-test
  (let [m (instance/instance ::birds {:name "Parroty"})]
    (are [m2 expected] (= expected
                          (merge m m2))
      {:type :parakeet} {:name "Parroty", :type :parakeet}
      nil               {:name "Parroty"})))

(deftest ^:parallel changes-do-not-ignore-nil-test
  (testing "changes should not ignore keys set to nil (#145)"
    (is (= {:location nil}
           (#'instance/changes* {:type "cockatiel"}
                                {:type "cockatiel", :location nil})))
    (is (nil? (#'toucan2.instance/changes* {:type "cockatiel", :location nil}
                                           {:type "cockatiel"})))
    (let [m  (instance/instance :bird {:type "cockatiel"})
          m2 (assoc m :location nil)]
      (is (= {:location nil}
             (protocols/changes m2))))))
