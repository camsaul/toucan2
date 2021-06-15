(ns toucan2.hydrate-test
  (:require [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]
            [toucan2.helpers :as helpers]
            [toucan2.hydrate :as hydrate]
            [toucan2.instance :as instance]
            [toucan2.tableable :as tableable]
            [toucan2.test :as test]))

(use-fixtures :once test/do-with-test-data)
(use-fixtures :each test/do-with-default-connection)

(helpers/define-table-name ::venues-with-category-keyword
  "venues")

(derive ::venues-with-category-keyword :toucan2/transformed)

(helpers/deftransforms ::venues-with-category-keyword
  {:category {:in  name
              :out keyword}})

(m/defmethod hydrate/table-for-automagic-hydration* [:default :default ::user]
  [_ _ _]
  :user)

(m/defmethod hydrate/table-for-automagic-hydration* [:default :default ::venue]
  [_ _ _]
  ::venues-with-category-keyword)

(deftest fk-keys-for-automagic-hydration*-test
  (testing "Should use hydrated key + `-id` by default"
    (is (= [:user-id]
           (hydrate/fk-keys-for-automagic-hydration* nil :checkins :user :users)))
    (testing "Should ignore keyword namespaces"
      (is (= [:venue-id]
             (hydrate/fk-keys-for-automagic-hydration* nil :checkins ::venue :venues))))))

(deftest can-hydrate-with-strategy-test
  (testing "should fail for unknown keys"
    (is (= false
           (hydrate/can-hydrate-with-strategy?* nil nil ::hydrate/automagic-batched :a)))))

;; custom automagic hydration

(m/defmethod hydrate/table-for-automagic-hydration* [:default ::hydrate-venue-with-people ::venue]
  [_ _ _]
  :people)

(m/defmethod hydrate/fk-keys-for-automagic-hydration* [:default ::hydrate-venue-with-people :default :default]
  [_ _ _ _]
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

    (testing "dispatch off of tableable -- hydrate different Tables for different instances"
      (is (= [(instance/instance :a-place {:venue-id 1
                                           ::venue   {:category :bar, :name "Tempest", :id 1}})
              (instance/instance :a-place {:venue-id 2
                                           ::venue   {:category :bar, :name "Ho's Tavern", :id 2}})]
             (remove-venues-timestamps
              (hydrate/hydrate [(instance/instance :a-place {:venue_id 1})
                                (instance/instance :a-place {:venue-id 2})]
                               ::venue))))
      (is (= [(instance/instance :toucan2.hydrate-test/hydrate-venue-with-people
                                 {:venue-id 1, :toucan2.hydrate-test/venue (instance/instance :people {:id 1, :name "Cam"})})
              (instance/instance :toucan2.hydrate-test/hydrate-venue-with-people
                                 {:venue-id 1000, :toucan2.hydrate-test/venue nil})]
             (remove-venues-timestamps
              (hydrate/hydrate [(instance/instance ::hydrate-venue-with-people {:venue_id 1})
                                (instance/instance ::hydrate-venue-with-people {:venue-id 1000})]
                               ::venue)))))))

(defn- valid-form? [form]
  (try
    (with-redefs [hydrate/hydrate-key (fn [_ _ results k]
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

(m/defmethod hydrate/simple-hydrate* [:default :default ::x]
  [_ _ _ {:keys [id], :as row}]
  (assoc row ::x id))

(m/defmethod hydrate/simple-hydrate* [:default :default ::y]
  [_ _ _ {:keys [id2], :as row}]
  (assoc row ::y id2))

(m/defmethod hydrate/simple-hydrate* [:default :default ::z]
  [_ _ _ {:keys [n], :as row}]
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

(m/defmethod hydrate/batched-hydrate* [:default :default ::is-bird?]
  [_ _ _ rows]
  (for [row rows]
    (assoc row ::is-bird? true)))

(deftest batched-hydration-test
  (testing "Check that batched hydration doesn't try to hydrate fields that already exist and are not delays"
    (is (= (toucan2.instance/instance :toucan2/default :user {:user-id 1, :user "OK <3"})
           (hydrate/hydrate (instance/instance :user {:user_id 1
                                                      :user    "OK <3"})
                            :user))))
  (is (= [{:type :toucan, ::is-bird? true}
          {:type :pigeon, ::is-bird? true}]
         (hydrate/hydrate [(instance/instance :bird {:type :toucan})
                           (instance/instance :bird {:type :pigeon})]
                          ::is-bird?))))

(m/defmethod tableable/primary-key* [:default :toucan2.hydrate-test.people/composite-pk]
  [_ _]
  [:id :name])

(m/defmethod hydrate/table-for-automagic-hydration* [:default :default ::people]
  [_ _ _]
  :toucan2.hydrate-test.people/composite-pk)

(m/defmethod hydrate/fk-keys-for-automagic-hydration* [:default :default ::people :default]
  [_ _ _ _]
  [:person-id :person-name])

(deftest automagic-batched-hydration-composite-pks-test
  (test/with-default-connection
    (is (= [(toucan2.instance/instance nil {:person-id                    1
                                             :person-name                  "Cam"
                                             :toucan2.hydrate-test/people (toucan2.instance/instance
                                                                            :toucan2.hydrate-test.people/composite-pk
                                                                            {:id         1
                                                                             :name       "Cam"
                                                                             :created-at (t/offset-date-time "2020-04-21T23:56Z")})})
            {:person-id                    2
             :person-name                  "Sam"
             :toucan2.hydrate-test/people (toucan2.instance/instance
                                            :toucan2.hydrate-test.people/composite-pk
                                            {:id         2
                                             :name       "Sam"
                                             :created-at (t/offset-date-time "2019-01-11T23:56Z")})}
            {:person-id 3, :person-name "Bird"}
            {:persion-id nil, :person-name "Pam"}]
           ;; should use the connectable from the first row
           (hydrate/hydrate [(instance/instance :test/postgres nil {:person-id 1, :person-name "Cam"})
                             {:person-id 2, :person-name "Sam"}
                             {:person-id 3, :person-name "Bird"}
                             {:persion-id nil, :person-name "Pam"}] ::people)))))
