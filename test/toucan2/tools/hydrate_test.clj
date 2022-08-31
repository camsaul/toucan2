(ns toucan2.tools.hydrate-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.execute :as execute]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.test :as test]
   [toucan2.tools.hydrate :as hydrate]
   [toucan2.tools.transformed :as transformed])
  (:import
   (java.time OffsetDateTime)))

(set! *warn-on-reflection* true)

(derive ::venues ::test/venues)

(derive ::venues.category-keyword ::venues)

(derive ::people ::test/people)

(transformed/deftransforms ::venues.category-keyword
  {:category {:in  name
              :out keyword}})

(m/defmethod hydrate/model-for-automagic-hydration [:default ::user]
  [_original-model _k]
  :user)

(m/defmethod hydrate/model-for-automagic-hydration [:default ::venue]
  [_original-model _k]
  ::venues.category-keyword)

(deftest fk-keys-for-automagic-hydration-test
  (testing "Should use hydrated key + `-id` by default"
    (is (= [:user-id]
           (hydrate/fk-keys-for-automagic-hydration :checkins :user :users)))
    (testing "Should ignore keyword namespaces"
      (is (= [:venue-id]
             (hydrate/fk-keys-for-automagic-hydration :checkins ::venue :venues))))))

(deftest can-hydrate-with-strategy-test
  (testing "should fail for unknown keys"
    (is (= false
           (hydrate/can-hydrate-with-strategy? nil ::hydrate/automagic-batched :a)))))

;; custom automagic hydration

(m/defmethod hydrate/model-for-automagic-hydration [::hydrate-venue-with-people ::venue]
  [_original-model _k]
  ::people)

(m/defmethod hydrate/fk-keys-for-automagic-hydration [::hydrate-venue-with-people :default :default]
  [_original-model _dest-key _hydrated-model]
  [:venue_id])

(deftest automagic-hydration-test
  (letfn [(remove-venues-timestamps [rows]
            (for [result rows]
              (update result ::venue #(dissoc % :updated-at :created-at))))]
    (is (= [{:venue-id 1
             ::venue   {:category :bar, :name "Tempest", :id 1}}
            {:venue-id 2
             ::venue   {:category :bar, :name "Ho's Tavern", :id 2}}]
           (remove-venues-timestamps
            (hydrate/hydrate [{:venue-id 1} {:venue-id 2}] ::venue))))

    (testing "dispatch off of model -- hydrate different Tables for different instances"
      (is (= [(instance/instance :a-place {:venue-id 1
                                           ::venue   {:category :bar, :name "Tempest", :id 1}})
              (instance/instance :a-place {:venue-id 2
                                           ::venue   {:category :bar, :name "Ho's Tavern", :id 2}})]
             (remove-venues-timestamps
              (hydrate/hydrate [(instance/instance :a-place {:venue_id 1})
                                (instance/instance :a-place {:venue-id 2})]
                               ::venue))))
      (is (= [(instance/instance ::hydrate-venue-with-people
                                 {:venue-id 1, ::venue (instance/instance ::people {:id 1, :name "Cam"})})
              (instance/instance ::hydrate-venue-with-people
                                 {:venue-id 1000, ::venue nil})]
             (remove-venues-timestamps
              (hydrate/hydrate [(instance/instance ::hydrate-venue-with-people {:venue_id 1})
                                (instance/instance ::hydrate-venue-with-people {:venue-id 1000})]
                               ::venue)))))))

(defn- valid-form? [form]
  (try
    (with-redefs [hydrate/hydrate-key (fn [_model results k]
                                        (for [result results]
                                          (assoc result k {})))]
      (hydrate/hydrate [{}] form))
    true
    (catch Throwable _e
      false)))

(deftest valid-form-test
  (testing "invalid forms"
    (doseq [form ["k"
                  'k
                  ['k :k2]
                  [:k 'k2]
                  [:k [:k2 [:k3]] :k4]
                  [:k [:k2] :k3]
                  [:k [:k2]]
                  [:k [[:k2]]]
                  [:k]
                  [[:k]]]]
      (is (= false
             (valid-form? form)))))
  (testing "valid forms"
    (doseq [form [:k
                  [:k :k2]
                  [:k [:k2 :k3] :k4]]]
      (is (= true
             (valid-form? form))))))

(m/defmethod hydrate/simple-hydrate [:default ::x]
  [_model _k {:keys [id], :as row}]
  (assoc row ::x id))

(m/defmethod hydrate/simple-hydrate [:default ::y]
  [_model _k {:keys [id2], :as row}]
  (assoc row ::y id2))

(m/defmethod hydrate/simple-hydrate [:default ::z]
  [_model _k {:keys [n], :as row}]
  (assoc row ::z (vec (for [i (range n)]
                        {:id i}))))

(deftest hydrate-key-seq-test
  (testing "check with a nested hydration that returns one result"
    (is (= [{:f {:id 1, ::x 1}}]
           (#'hydrate/hydrate-key-seq
            [{:f {:id 1}}]
            [:f ::x]))))
  (is (= [{:f {:id 1, ::x 1}}
          {:f {:id 2, ::x 2}}]
         (#'hydrate/hydrate-key-seq
          [{:f {:id 1}}
           {:f {:id 2}}]
          [:f ::x])))
  (testing "check with a nested hydration that returns multiple results"
    (is (= [{:f [{:id 1, ::x 1}
                 {:id 2, ::x 2}
                 {:id 3, ::x 3}]}]
           (#'hydrate/hydrate-key-seq
            [{:f [{:id 1}
                  {:id 2}
                  {:id 3}]}]
            [:f ::x])))))

(deftest hydrate-key-test
  (is (= [{:id 1, ::x 1}
          {:id 2, ::x 2}
          {:id 3, ::x 3}]
         (#'hydrate/hydrate-key
          nil
          [{:id 1}
           {:id 2}
           {:id 3}] ::x))))

(deftest hydrate-test-2
  (testing "make sure we can do basic hydration"
    (is (= {:a 1, :id 2, ::x 2}
           (hydrate/hydrate {:a 1, :id 2}
                            ::x))))
  (testing "specifying \"nested\" hydration with no \"nested\" keys should throw an exception and tell you not to do it"
    (is (= (str "Invalid hydration form: replace [:b] with :b. Vectors are for nested hydration. "
                "There's no need to use one when you only have a single key.")
           (try (hydrate/hydrate {:a 1, :id 2}
                                 [:b])
                (catch Throwable e
                  (.getMessage e))))))
  (testing "check that returning an array works correctly"
    (is (= {:n  3
            ::z [{:id 0}
                 {:id 1}
                 {:id 2}]}
           (hydrate/hydrate {:n 3} ::z))))
  (testing "check that nested keys aren't hydrated if we don't ask for it"
    (is (= {:d {:id 1}}
           (hydrate/hydrate {:d {:id 1}}
                            :d))))
  (testing "check that nested keys can be hydrated if we DO ask for it"
    (is (= {:d {:id 1, ::x 1}}
           (hydrate/hydrate {:d {:id 1}}
                            [:d ::x])))
    (testing "check that nested hydration also works if one step returns multiple results"
      (is (= {:n  3
              ::z [{:id 0, ::x 0}
                   {:id 1, ::x 1}
                   {:id 2, ::x 2}]}
             (hydrate/hydrate {:n 3} [::z ::x])))))
  (testing "check nested hydration with nested maps"
    (is (= [{:f {:id 1, ::x 1}}
            {:f {:id 2, ::x 2}}
            {:f {:id 3, ::x 3}}
            {:f {:id 4, ::x 4}}]
           (hydrate/hydrate [{:f {:id 1}}
                             {:f {:id 2}}
                             {:f {:id 3}}
                             {:f {:id 4}}] [:f ::x]))))
  (testing "check that hydration works with top-level nil values"
    (is (= [{:id 1, ::x 1}
            {:id 2, ::x 2}
            nil
            {:id 4, ::x 4}]
           (hydrate/hydrate [{:id 1}
                             {:id 2}
                             nil
                             {:id 4}] ::x))))
  (testing "check nested hydration with top-level nil values"
    (is (= [{:f {:id 1, ::x 1}}
            {:f {:id 2, ::x 2}}
            nil
            {:f {:id 4, ::x 4}}]
           (hydrate/hydrate [{:f {:id 1}}
                             {:f {:id 2}}
                             nil
                             {:f {:id 4}}] [:f ::x]))))
  (testing "check that nested hydration w/ nested nil values"
    (is (= [{:f {:id 1, ::x 1}}
            {:f {:id 2, ::x 2}}
            {:f nil}
            {:f {:id 4, ::x 4}}]
           (hydrate/hydrate [{:f {:id 1}}
                             {:f {:id 2}}
                             {:f nil}
                             {:f {:id 4}}] [:f ::x])))
    (is (= [{:f {:id 1, ::x 1}}
            {:f {:id 2, ::x 2}}
            {:f {:id nil, ::x nil}}
            {:f {:id 4, ::x 4}}]
           (hydrate/hydrate [{:f {:id 1}}
                             {:f {:id 2}}
                             {:f {:id nil}}
                             {:f {:id 4}}] [:f ::x]))))
  (testing "check that it works with some objects missing the key"
    (is (= [{:f [{:id 1, ::x 1}
                 {:id 2, ::x 2}
                 {:g 3, ::x nil}]}
            {:f [{:id 1, ::x 1}]}
            {:f [{:id 4, ::x 4}
                 {:g 5, ::x nil}
                 {:id 6, ::x 6}]}]
           (hydrate/hydrate [{:f [{:id 1}
                                  {:id 2}
                                  {:g 3}]}
                             {:f [{:id 1}]}
                             {:f [{:id 4}
                                  {:g 5}
                                  {:id 6}]}] [:f ::x]))))
  (testing "nested-nested hydration"
    (is (= [{:f [{:g {:id 1, ::x 1}}
                 {:g {:id 2, ::x 2}}
                 {:g {:id 3, ::x 3}}]}
            {:f [{:g {:id 4, ::x 4}}
                 {:g {:id 5, ::x 5}}]}]
           (hydrate/hydrate
            [{:f [{:g {:id 1}}
                  {:g {:id 2}}
                  {:g {:id 3}}]}
             {:f [{:g {:id 4}}
                  {:g {:id 5}}]}]
            [:f [:g ::x]]))))
  (testing "nested + nested-nested hydration"
    (is (= [{:f [{:id 1, :g {:id 1, ::x 1}, ::x 1}]}
            {:f [{:id 2, :g {:id 4, ::x 4}, ::x 2}
                 {:id 3, :g {:id 5, ::x 5}, ::x 3}]}]
           (hydrate/hydrate [{:f [{:id 1, :g {:id 1}}]}
                             {:f [{:id 2, :g {:id 4}}
                                  {:id 3, :g {:id 5}}]}]
                            [:f ::x [:g ::x]]))))
  (testing "make sure nested-nested hydration doesn't accidentally return maps where there were none"
    (is (= {:f [{:h {:id 1, ::x 1}}
                {}
                {:h {:id 3, ::x 3}}]}
           (hydrate/hydrate {:f [{:h {:id 1}}
                                 {}
                                 {:h {:id 3}}]}
                            [:f [:h ::x]]))))
  (testing "check nested hydration with several keys"
    (is (= [{:f [{:id 1, :h {:id 1, :id2 1, ::x 1, ::y 1}, ::x 1}]}
            {:f [{:id 2, :h {:id 4, :id2 2, ::x 4, ::y 2}, ::x 2}
                 {:id 3, :h {:id 5, :id2 3, ::x 5, ::y 3}, ::x 3}]}]
           (hydrate/hydrate [{:f [{:id 1, :h {:id 1, :id2 1}}]}
                             {:f [{:id 2, :h {:id 4, :id2 2}}
                                  {:id 3, :h {:id 5, :id2 3}}]}]
                            [:f ::x [:h ::x ::y]]))))
  (testing "multiple nested-nested hydrations"
    (is (= [{:f [{:g {:id 1, ::x 1}, :h {:i {:id2 1, ::y 1}}}]}
            {:f [{:g {:id 2, ::x 2}, :h {:i {:id2 2, ::y 2}}}
                 {:g {:id 3, ::x 3}, :h {:i {:id2 3, ::y 3}}}]}]
           (hydrate/hydrate [{:f [{:g {:id 1}
                                   :h {:i {:id2 1}}}]}
                             {:f [{:g {:id 2}
                                   :h {:i {:id2 2}}}
                                  {:g {:id 3}
                                   :h {:i {:id2 3}}}]}]
                            [:f [:g ::x] [:h [:i ::y]]]))))
  (testing "check that hydration doesn't barf if we ask it to hydrate an object that's not there"
    (is (= {:f [:a 100]}
           (hydrate/hydrate {:f [:a 100]} :p)))))

(m/defmethod hydrate/batched-hydrate [:default ::is-bird?]
  [_model _k rows]
  (for [row rows]
    (assoc row ::is-bird? true)))

(deftest batched-hydration-test
  (testing "Check that batched hydration doesn't try to hydrate fields that already exist and are not delays"
    (is (= (instance/instance :user {:user-id 1, :user "OK <3"})
           (hydrate/hydrate (instance/instance :user {:user_id 1
                                                      :user    "OK <3"})
                            :user))))
  (is (= [{:type :toucan, ::is-bird? true}
          {:type :pigeon, ::is-bird? true}]
         (hydrate/hydrate [(instance/instance :bird {:type :toucan})
                           (instance/instance :bird {:type :pigeon})]
                          ::is-bird?))))

(derive ::people.composite-pk ::people)

(m/defmethod model/primary-keys ::people.composite-pk
  [_model]
  [:id :name])

(m/defmethod hydrate/model-for-automagic-hydration [:default ::people]
  [_original-model _k]
  ::people.composite-pk)

(m/defmethod hydrate/fk-keys-for-automagic-hydration [:default ::people :default]
  [_original-model _dest-key _hydrated-model]
  [:person-id :person-name])

(deftest automagic-batched-hydration-composite-pks-test
  (is (= [(instance/instance nil {:person-id   1
                                  :person-name "Cam"
                                  ::people     (instance/instance
                                                ::people.composite-pk
                                                {:id         1
                                                 :name       "Cam"
                                                 :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")})})
          {:person-id   2
           :person-name "Sam"
           ::people     (instance/instance
                         ::people.composite-pk
                         {:id         2
                          :name       "Sam"
                          :created-at (OffsetDateTime/parse "2019-01-11T23:56Z")})}
          {:person-id 3, :person-name "Bird"}
          {:persion-id nil, :person-name "Pam"}]
         (hydrate/hydrate [(instance/instance nil {:person-id 1, :person-name "Cam"})
                           {:person-id 2, :person-name "Sam"}
                           {:person-id 3, :person-name "Bird"}
                           {:persion-id nil, :person-name "Pam"}]
                          ::people))))

(deftest automagic-batch-hydration-composite-pks-dont-fetch-nil-test
  (testing "automagic batched hydration should not try to fetch objects when some or all FK keys are nil"
    (testing "sanity check"
      (execute/with-call-count [call-count]
        (is (= {:person-id   1
                :person-name "Cam"
                ::people     {:id         1
                              :name       "Cam"
                              :created-at (OffsetDateTime/parse "2020-04-21T23:56Z")}}
               (hydrate/hydrate (instance/instance nil {:person-id 1, :person-name "Cam"})
                                ::people)))
        (is (= 1
               (call-count)))))
    (testing "some or all keys are nil"
      (doseq [m [{:person-id 1, :person-name nil}
                 {:person-id nil, :person-name "Cam"}
                 {:person-id nil, :person-name nil}]]
        (testing (format "source map = %s" (pr-str m))
          (execute/with-call-count [call-count]
            (is (= m
                   (hydrate/hydrate (instance/instance nil m)
                                    ::people)))
            (is (= 0
                   (call-count)))))))))

(derive ::birds.boolean-pk ::test/birds)

(m/defmethod model/primary-keys ::birds.boolean-pk
  [_model]
  [:good-bird])

(m/defmethod hydrate/model-for-automagic-hydration [:default ::birb]
  [_original-model _k]
  ::birds.boolean-pk)

(m/defmethod hydrate/fk-keys-for-automagic-hydration [:default ::birb ::birds.boolean-pk]
  [_original-model _dest-key _hydrated-model]
  [:good-bird?])

(deftest automagic-batched-hydration-truthiness-test
  (testing "Make sure automagic batched hydration compares things with some? (should work with false values)")
  (conn/with-connection [_conn ::test/db]
    (let [results                      [{:good-bird? true}
                                        {:good-bird? false}
                                        {:good-bird? nil}]
          ;; which bird we get back is indeterminate since there are multiple matching birds; we will consider any of
          ;; them to be the right answer.
          good-birds                   #{{:id 1, :name "Reggae", :bird-type "toucan", :good-bird true}
                                         {:id 2, :name "Lucky", :bird-type "pigeon", :good-bird true}
                                         {:id 3, :name "Parroty", :bird-type "parakeet", :good-bird true}}
          bad-birds                    #{{:id 4, :name "Green Friend", :bird-type "parakeet", :good-bird false}
                                         {:id 5, :name "Parrot Hilton", :bird-type "parakeet", :good-bird false}}
          [good-bird bad-bird no-bird] (hydrate/hydrate results ::birb)]
      (testing "good bird"
        (is (= #{:good-bird? ::birb}
               (set (keys good-bird))))
        (is (= true
               (:good-bird? good-bird)))
        (is (contains? good-birds (::birb good-bird))))
      (testing "bad bird"
        (is (= #{:good-bird? ::birb}
               (set (keys bad-bird))))
        (is (= false
               (:good-bird? bad-bird)))
        (is (contains? bad-birds (::birb bad-bird))))
      (testing "don't hydrate nil keys"
        (is (= {:good-bird? nil}
               no-bird))))))

(deftest unnest-first-result-test
  (are [coll expected] (= expected
                          (#'hydrate/unnest-first-result coll))
    nil        nil
    :a         nil
    []         nil
    [:a]       :a
    [[:a]]     :a
    [[[:a]]]   :a
    [[[[:a]]]] :a))

(m/defmethod hydrate/simple-hydrate [:default ::k]
  [_model k m]
  (assoc m k 5))

(deftest nested-hydrate-sequence-test
  (testing "Should be able to do nested hydration for sequences"
    (is (= {:m [(instance/instance ::m.hydrate-sequence {:a 1, ::k 5})
                (instance/instance ::m.hydrate-sequence {:a 2, ::k 5})]}
           (hydrate/hydrate
            {:m [(instance/instance ::m.hydrate-sequence {:a 1})
                 (instance/instance ::m.hydrate-sequence {:a 2})]}
            [:m ::k])))))

(m/defmethod hydrate/simple-hydrate [::m.hydrate-sequence ::model.x]
  [_model k m]
  (assoc m k 1000))

(deftest hydrate-sequence-dispatch-on-model-test
  (testing "We should dispatch on the model of the first instance when hydrating a sequence"
    (is (= [(instance/instance ::m.hydrate-sequence {:a 1, ::model.x 1000})
            (instance/instance ::m.hydrate-sequence {:a 2, ::model.x 1000})]
           (hydrate/hydrate
            [(instance/instance ::m.hydrate-sequence {:a 1})
             (instance/instance ::m.hydrate-sequence {:a 2})]
            ::model.x)))
    (testing "Should work when doing recursive hydration"
      (is (= {:m [(instance/instance ::m.hydrate-sequence {:a 1, ::model.x 1000})
                  (instance/instance ::m.hydrate-sequence {:a 2, ::model.x 1000})]}
             (hydrate/hydrate
              {:m [(instance/instance ::m.hydrate-sequence {:a 1})
                   (instance/instance ::m.hydrate-sequence {:a 2})]}
              [:m ::model.x]))))))

(deftest flatten-unflatten-test
  (are [form] (= form
                 (#'hydrate/unflatten-collection (#'hydrate/flatten-collection form)))
    nil
    []
    [{:a 1}]
    [{:a 1} {:b 2}]
    [[]]
    [[{:a 1} {:b 2}] {:c 3} {:d 4}]
    [[[]]]
    [[[{:a 1} {:b 2}] {:c 3} {:d 4} {:e 5} {:f 6}]]
    [[[[]]]]
    [[[[{:a 1} {:b 2}] {:c 3} {:d 4} {:e 5} {:f 6}] {:g 7} {:h 8}]]))

(deftest preserve-shape-test
  (testing "hydration should preserve the shape/nesting of the original form"
    (are [form expected] (= expected
                            (hydrate/hydrate form ::k))
      nil
      nil

      []
      []

      [{:a 1}]
      [{:a 1, ::k 5}]

      [{:a 1} {:b 2}]
      [{:a 1, ::k 5} {:b 2, ::k 5}]

      [[]]
      [[]]

      [[{:a 1} {:b 2}] {:c 3} {:d 4}]
      [[{:a 1, ::k 5} {:b 2, ::k 5}] {:c 3, ::k 5} {:d 4, ::k 5}]

      [[[]]]
      [[[]]]

      [[[{:a 1} {:b 2}] {:c 3} {:d 4}] {:e 5} {:f 6}]
      [[[{:a 1, ::k 5} {:b 2, ::k 5}] {:c 3, ::k 5} {:d 4, ::k 5}] {:e 5, ::k 5} {:f 6, ::k 5}]

      [[[[]]]]
      [[[[]]]]

      [[[[{:a 1} {:b 2}] {:c 3} {:d 4}] {:e 5} {:f 6}] {:g 7} {:h 8}]
      [[[[{:a 1, ::k 5} {:b 2, ::k 5}] {:c 3, ::k 5} {:d 4, ::k 5}] {:e 5, ::k 5} {:f 6, ::k 5}] {:g 7, ::k 5} {:h 8, ::k 5}])))
