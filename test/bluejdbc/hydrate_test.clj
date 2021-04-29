(ns bluejdbc.hydrate-test
  (:require [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)
(use-fixtures :each test/do-with-default-connection)

(deftest kw-append-test
  (is (= :user_id
         (#'hydrate/kw-append :user "_id")))
  (is (= :toucan-id
         (#'hydrate/kw-append :toucan "-id"))))

(m/defmethod hydrate/automagic-hydration-key-table ::user
  [_]
  :user)

(m/defmethod table-aware/select* :after [:default :venues/category-keyword :default]
  [_ _ reducible-query _]
  (eduction
   (map (fn [venue]
          (cond-> venue
            (:category venue) (update :category keyword))))
   reducible-query))

(m/defmethod hydrate/automagic-hydration-key-table ::venue
  [_]
  :venues/category-keyword)

(deftest can-hydrate-with-strategy-test
  (testing "should fail for unknown keys"
    (is (= false
           (hydrate/can-hydrate-with-strategy? ::hydrate/automagic-batched [{:a_id 1} {:a_id 2}] :a))))
  (testing "should work for known keys if k_id present in every map"
    (is (= true
           (hydrate/can-hydrate-with-strategy? ::hydrate/automagic-batched [{::user_id 1} {::user_id 2}] ::user))))
  (testing "should work for both k_id and k-id style keys"
    (is (= true
           (hydrate/can-hydrate-with-strategy? ::hydrate/automagic-batched [{::user-id 1} {::user-id 2}] ::user))))
  (testing "should fail for known keys if k_id isn't present in every map"
    (is (= false
           (hydrate/can-hydrate-with-strategy? ::hydrate/automagic-batched
                                               [{::user_id 1} {::user_id 2} {:some-other-key 3}]
                                               ::user)))))

(deftest hydrate-test
  (testing "it should correctly hydrate"
    (is (= [{::venue_id 1
             ::venue    {:category :bar, :name "Tempest", :id 1}}
            {::venue-id 2
             ::venue    {:category :bar, :name "Ho's Tavern", :id 2}}]
           (for [result (hydrate/hydrate [{::venue_id 1} {::venue-id 2}] ::venue)]
             (update result ::venue #(dissoc % :updated-at :created-at)))))))

(defn- valid-form? [form]
  (try
    (with-redefs [hydrate/hydrate-key (fn [results k]
                                        (for [result results]
                                          (assoc result k {})))]
      (hydrate/hydrate [{}] form))
    true
    (catch Throwable e
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
  [{:keys [id]} _]
  id)

(m/defmethod hydrate/simple-hydrate [:default ::y]
  [{:keys [id2]} _]
  id2)

(m/defmethod hydrate/simple-hydrate [:default ::z]
  [{:keys [n]} _]
  (vec (for [i (range n)]
         {:id i})))

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
  [objects _]
  (for [object objects]
    (assoc object ::is-bird? true)))

(deftest batched-hydration-test
  (testing "Check that batched hydration doesn't try to hydrate fields that already exist and are not delays"
    (is (= {:user_id 1
            :user    "OK <3"}
           (hydrate/hydrate (instance/instance :user {:user_id 1
                                                      :user    "OK <3"})
                            :user))))
  (is (= [{:type :toucan, ::is-bird? true}
          {:type :pigeon, ::is-bird? true}]
         (hydrate/hydrate [(instance/instance :bird {:type :toucan})
                           (instance/instance :bird {:type :pigeon})]
                          ::is-bird?))))

;; TODO add test for selecting hydration for where (not= pk :id)
