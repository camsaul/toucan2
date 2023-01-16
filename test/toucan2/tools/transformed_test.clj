(ns toucan2.tools.transformed-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.log :as log]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.save :as save]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.before-update :as before-update]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.identity-query :as identity-query]
   [toucan2.tools.transformed :as transformed]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(doto ::venues.category-keyword
  (derive ::test/venues)
  (derive ::transformed/transformed.model))

(m/defmethod transformed/transforms ::venues.category-keyword
  [_model]
  {:category {:in  (fn [k]
                     (testing "Category [in] should still be a keyword (not transformed yet)"
                       (is (keyword? k)))
                     (name k))
              :out (fn [s]
                     (testing "Category [out] should still be a string (not transformed yet)"
                       (is (string? s)))
                     (keyword s))}})

(derive ::venues.string-id-and-category-keyword ::venues.category-keyword)

(m/defmethod transformed/transforms ::venues.string-id-and-category-keyword
  [_model]
  {:id {:in  parse-long
        :out str}})

(deftest ^:parallel select-in-test
  (testing "select should transform values going in"
    (testing "key-value condition"
      (is (= [{:id 1, :name "Tempest", :category :bar}
              {:id 2, :name "Ho's Tavern", :category :bar}]
             (select/select ::venues.category-keyword :category :bar {:select   [:id :name :category]
                                                                      :order-by [[:id :asc]]})))
      (testing "Toucan-style [f & args] condition\n"
        (are [condition] (= [{:id 1, :name "Tempest", :category :bar}
                             {:id 2, :name "Ho's Tavern", :category :bar}]
                            (select/select ::venues.category-keyword :category condition
                                           {:select   [:id :name :category]
                                            :order-by [[:id :asc]]}))
          [:= :bar]
          [:in [:bar]]
          [:in [:bar :saloon]]
          [:in #{:bar :saloon}]
          [:in (list :bar :saloon)])))
    (testing "as the PK"
      (testing "(single value)"
        (is (= [{:id "1", :name "Tempest", :category :bar}]
               (select/select ::venues.string-id-and-category-keyword :toucan/pk "1" {:select [:id :name :category]}))))
      (testing "(vector of multiple values)"
        (is (= [{:id "1", :name "Tempest", :category :bar}]
               (select/select ::venues.string-id-and-category-keyword :toucan/pk ["1"] {:select [:id :name :category]})))))))

(deftest ^:parallel select-out-test
  (testing "select should transform values coming out"
    (is (= [{:id 1, :name "Tempest", :category :bar}
            {:id 2, :name "Ho's Tavern", :category :bar}
            {:id 3, :name "BevMo", :category :store}]
           (select/select ::venues.category-keyword {:select [:id :name :category]})))
    (testing "should work if transformed key is not present in results"
      (is (= [{:id 1, :name "Tempest"}
              {:id 2, :name "Ho's Tavern"}
              {:id 3, :name "BevMo"}]
             (select/select ::venues.category-keyword {:select [:id :name], :order-by [[:id :asc]]}))))
    (testing "should work with select-one and other special functions"
      (is (= :bar
             (select/select-one-fn :category ::venues.category-keyword :id 1)))
      (is (= #{:store :bar}
             (select/select-fn-set :category ::venues.category-keyword))))
    (testing "Transformed version of the map should be considered the instance 'original'"
      (let [instance (select/select-one ::venues.category-keyword :toucan/pk 1 {:select [:id :name :category]})]
        (is (= {:id 1, :name "Tempest", :category :bar}
               (protocols/original instance)))
        (is (= nil
               (protocols/changes instance))))))
  (testing "composed transforms"
    (is (= (instance/instance
            ::venues.string-id-and-category-keyword
            {:id         "1"
             :name       "Tempest"
             :category   :bar
             :created-at (LocalDateTime/parse "2017-01-01T00:00")
             :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
           (select/select-one ::venues.string-id-and-category-keyword :toucan/pk "1")
           (select/select-one ::venues.string-id-and-category-keyword {:order-by [[:id :asc]]})))))

(m/defmethod transformed/transforms ::unnormalized
  [_model]
  {:un_normalized {:in name, :out keyword}})

(deftest ^:parallel apply-row-transform-test
  (doseq [[message m] {"plain map" {:id 1}
                       "Instance"  (instance/instance :x {:id 1})
                       ;; TODO
                       #_          "custom IInstance" #_ (test.custom-types/->CustomIInstance {:id 1} {:id 1})}
          [message m] (list*
                       [message m]
                       ;; TODO
                       nil
                       #_(when (instance/instance? m)
                           (for [[row-message row] {"wrapping Row"         (row/row {:id (constantly 1)})
                                                    "wrapping custom IRow" (test.custom-types/->CustomIRow
                                                                            {:id (constantly 1)})}]
                             [(str message " " row-message)
                              (-> m
                                  (instance/with-original row)
                                  (instance/with-current row))])))]
    (testing message
      (let [m2 (#'transformed/apply-result-row-transform m :id str)]
        (testing "current value"
          (is (= {:id "1"}
                 m2)))
        (when (instance/instance? m)
          (testing "original value"
            (is (= {:id "1"}
                   (protocols/original m2))))
          (is (identical? (protocols/current m2)
                          (protocols/original m2))))))))

(deftest ^:synchronized update!-test
  (testing "key-value conditions"
    (test/with-discarded-table-changes :venues
      (is (= 2
             (update/update! ::venues.category-keyword :category :bar {:category :BAR})))
      (is (= #{["Ho's Tavern" :BAR] ["Tempest" :BAR]}
             (select/select-fn-set (juxt :name :category) ::venues.category-keyword :category :BAR))))
    (testing "Toucan-style [f & args] condition"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (update/update! ::venues.category-keyword :category [:in [:bar]] {:category :BAR})))
        (is (= #{:store :BAR}
               (select/select-fn-set :category ::venues.category-keyword))))))
  (testing "conditions map"
    (test/with-discarded-table-changes :venues
      (is (= 2
             (update/update! ::venues.category-keyword {:category :bar} {:category :BAR})))))
  (testing "PK"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (update/update! ::venues.string-id-and-category-keyword "1" {:name "Wow"})))
      (is (= "Wow"
             (select/select-one-fn :name ::venues.category-keyword 1))))))

(derive ::venues.category-keyword.before-update ::venues.category-keyword)

(def ^:private ^:dynamic *venue-before-update* nil)

(before-update/define-before-update ::venues.category-keyword.before-update
  [venue]
  (when *venue-before-update*
    (reset! *venue-before-update* venue))
  venue)

(deftest ^:synchronized update!-before-update-test
  (testing "update! with transforms AND before-update"
    (test/with-discarded-table-changes :venues
      (binding [*venue-before-update* (atom nil)]
        (is (= 1
               (update/update! ::venues.category-keyword.before-update 1 {:category :BAR, :name "My Venue"})))
        (testing `*venue-before-update*
          (is (= {:id         1
                  :name       "My Venue"
                  :category   :BAR
                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                 @*venue-before-update*))
          (testing `protocols/original
            (is (= {:id         1
                    :name       "Tempest"
                    :category   :bar
                    :created-at (LocalDateTime/parse "2017-01-01T00:00")
                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                   (protocols/original @*venue-before-update*))))
          (testing `protocols/current
            (is (= {:id         1
                    :name       "My Venue"
                    :category   :BAR
                    :created-at (LocalDateTime/parse "2017-01-01T00:00")
                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                   (protocols/current @*venue-before-update*))))
          (testing `protocols/changes
            (is (= {:category :BAR, :name "My Venue"}
                   (protocols/changes @*venue-before-update*)))))
        (is (= {:id         1
                :name       "My Venue"
                :category   :BAR
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
               (select/select-one ::venues.category-keyword.before-update 1)))))))

(deftest ^:synchronized save!-test
  (test/with-discarded-table-changes :venues
    (let [venue (select/select-one ::venues.category-keyword 1)]
      (is (= {:category :dive-bar}
             (protocols/changes (assoc venue :category :dive-bar))))
      (is (= {:id         1
              :name       "Tempest"
              :category   :dive-bar
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse  "2017-01-01T00:00")}
             (save/save! (assoc venue :category :dive-bar))))
      (is (= {:id         1
              :name       "Tempest"
              :category   :dive-bar
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
             (select/select-one ::venues.category-keyword 1))))))

(deftest ^:synchronized insert!-test
  (doseq [insert! [#'insert/insert!
                   #'insert/insert-returning-pks!
                   #'insert/insert-returning-instances!]]
    (testing insert!
      (doseq [[args-description args] {"single map row"        [{:name "Hi-Dive", :category :bar}]
                                       "multiple map rows"     [[{:name "Hi-Dive", :category :bar}]]
                                       "kv args"               [:name "Hi-Dive", :category :bar]
                                       "columns + vector rows" [[:name :category] [["Hi-Dive" :bar]]]}]
        (testing (str args-description \newline (pr-str (list* insert! ::venues.category-keyword args)))
          (test/with-discarded-table-changes :venues
            (is (= (condp = insert!
                     #'insert/insert!                     1
                     #'insert/insert-returning-pks!       [4]
                     #'insert/insert-returning-instances! [(instance/instance
                                                            ::venues.category-keyword
                                                            {:id         4
                                                             :name       "Hi-Dive"
                                                             :category   :bar
                                                             :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                             :updated-at (LocalDateTime/parse "2017-01-01T00:00")})])
                   (apply insert! ::venues.category-keyword args)))
            (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
                   (select/select-fn-set :name ::venues.category-keyword :category :bar))))))
      (testing "transformed primary key"
        (test/with-discarded-table-changes :venues
          (is (= (condp = insert!
                   #'insert/insert!                     1
                   #'insert/insert-returning-pks!       ["4"]
                   #'insert/insert-returning-instances! [(instance/instance
                                                          ::venues.string-id-and-category-keyword
                                                          {:id         "4"
                                                           :name       "Hi-Dive"
                                                           :category   :bar
                                                           :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                           :updated-at (LocalDateTime/parse "2017-01-01T00:00")})])
                 (insert! ::venues.string-id-and-category-keyword [{:name "Hi-Dive", :category :bar}]))))))))

(deftest ^:synchronized transform-insert-returning-results-without-select-test
  (testing "insert-returning-instances results should be transformed if they come directly from the DB (not via select)"
    (test/with-discarded-table-changes :venues
      (binding [pipeline/*build*             (fn [_query-type _model parsed-args _resolved-query]
                                               parsed-args)
                pipeline/*compile*           (fn [_query-type _model built-query]
                                               built-query)
                pipeline/*transduce-execute* (fn [rf _query-type _model {:keys [rows], :as _compiled-query}]
                                               {:pre [(seq rows)]}
                                               (transduce identity rf (map (fn [row]
                                                                             (update row :category name))
                                                                           rows)))]
        (is (= (instance/instance ::venues.category-keyword
                                  {:name "BevLess", :category :bar})
               (pipeline/transduce-with-model
                (completing conj first)
                :toucan.query-type/insert.instances
                ::venues.category-keyword
                {:rows [{:name "BevLess", :category :bar}]}))))
      (testing "sanity check: should not have inserted a row."
        (is (= 3
               (select/count ::test/venues)))))))

(deftest ^:synchronized delete!-test
  (testing `pipeline/build
    (is (= {:delete-from [:venues]
            :where       [:= :category "bar"]}
           (pipeline/build :toucan.query-type/delete.update-count ::venues.category-keyword {:kv-args {:category :bar}} {}))))
  (testing "Delete row by PK"
    (test/with-discarded-table-changes :venues
      (is (= 1
             (delete/delete! ::venues.string-id-and-category-keyword :toucan/pk "1")))
      (is (= []
             (select/select ::test/venues 1)))
      (is (= #{2}
             (select/select-fn-set :id ::test/venues 2)))))
  (testing "Delete row by key-value conditions"
    (test/with-discarded-table-changes :venues
      (is (= 2
             (delete/delete! ::venues.category-keyword :category :bar)))
      (is (= []
             (select/select ::venues.category-keyword :category :bar))))
    (testing "Toucan-style fn-args vector"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (delete/delete! ::venues.category-keyword :category [:in [:bar]])))
        (is (= []
               (select/select ::venues.category-keyword :category :bar)))))))

(deftest ^:synchronized no-npes-test
  (testing "Don't apply transforms to values that are nil (avoid NPEs)\n"
    (testing "in"
      (testing "insert rows"
        (test/with-discarded-table-changes :venues
          ;; this should still throw an error, but it shouldn't be an NPE from the transform.
          (is (thrown-with-msg?
               Exception
               (case (test/current-db-type)
                 :postgres #"ERROR: null value in column .* violates not-null constraint"
                 :h2       #"NULL not allowed for column \"CATEGORY\"")
               (insert/insert! ::venues.category-keyword {:name "No Category", :category nil})))))
      (testing "conditions"
        (is (= nil
               (select/select-one ::venues.category-keyword :category nil)))))
    (testing "out\n"
      (let [instance (select/select-one ::venues.category-keyword (identity-query/identity-query
                                                                   [{:id 1, :name "No Category", :category nil}]))]
        (is (= {:id 1, :name "No Category", :category nil}
               instance))))))

(derive ::venues-transform-in-only ::test/venues)
(derive ::venues-transform-out-only ::test/venues)

(transformed/deftransforms ::venues-transform-in-only
  {:category {:in name}})

(transformed/deftransforms ::venues-transform-out-only
  {:category {:out keyword}})

(deftest ^:synchronized one-way-transforms-test
  (testing "Transforms should still work if you only specify `:in` or only specify `:out`"
    (testing "in"
      (testing "insert rows"
        (test/with-discarded-table-changes :venues
          (is (= 1
                 (insert/insert! ::venues-transform-out-only {:name "Walgreens", :category "store"})))))
      (testing "conditions"
        (is (= nil
               (select/select-one ::venues-transform-out-only :category "pet-store")))))
    (testing "out"
      (is (= (instance/instance
              ::venues-transform-in-only
              {:id         3
               :name       "BevMo"
               :category   "store"
               :created-at (LocalDateTime/parse "2017-01-01T00:00")
               :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
             (select/select-one ::venues-transform-in-only :category :store))))))

;;;; deftransforms

(derive ::transformed-venues ::test/venues)

(transformed/deftransforms ::transformed-venues
  {:id {:in  parse-long
        :out str}})

(deftest ^:parallel deftransforms-test
  (is (= (instance/instance
          ::transformed-venues
          {:id         "1"
           :name       "Tempest"
           :category   "bar"
           :created-at (LocalDateTime/parse "2017-01-01T00:00")
           :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::transformed-venues :toucan/pk "1"))))

(transformed/deftransforms ::transformed-venues-2
  {:category {:in name, :out keyword}})

(derive ::venues.composed-deftransform ::test/venues)
(derive ::venues.composed-deftransform ::transformed-venues)
(derive ::venues.composed-deftransform ::transformed-venues-2)

;;; Once https://github.com/camsaul/methodical/issues/97 is in place this should no longer be needed.
(m/prefer-method! #'transformed/transforms ::transformed-venues-2 ::transformed-venues)

(deftest ^:parallel compose-deftransforms-test
  (is (= {:id {:in parse-long, :out str}}
         (transformed/transforms ::transformed-venues)))
  (is (= {:category {:in name, :out keyword}}
         (transformed/transforms ::transformed-venues-2)))
  (is (= {:id       {:in parse-long, :out str}
          :category {:in name, :out keyword}}
         (transformed/transforms ::venues.composed-deftransform)))
  (is (= (instance/instance
          ::venues.composed-deftransform
          {:id         "1"
           :name       "Tempest"
           :category   :bar
           :created-at (LocalDateTime/parse "2017-01-01T00:00")
           :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::venues.composed-deftransform :toucan/pk "1"))))

(derive ::venues.override-transforms ::venues.composed-deftransform)

(m/defmethod transformed/transforms :around ::venues.override-transforms
  [_model]
  {})

(deftest ^:parallel compose-deftransforms-override-test
  (is (= {}
         (transformed/transforms ::venues.override-transforms)))
  (is (= (instance/instance
          ::venues.override-transforms
          {:id         1
           :name       "Tempest"
           :category   "bar"
           :created-at (LocalDateTime/parse "2017-01-01T00:00")
           :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
         (select/select-one ::venues.override-transforms :toucan/pk 1))))

(derive ::categories.namespaced.category-keyword ::test/categories)

(transformed/deftransforms ::categories.namespaced.category-keyword
  {:category/name {:in name, :out keyword}})

(derive ::venues.namespaced.category-keyword ::test/venues)

(transformed/deftransforms ::venues.namespaced.category-keyword
  {:venue/category {:in name, :out keyword}})

;;; workaround for https://github.com/camsaul/methodical/issues/97
(doseq [varr [#'transformed/transforms]]
  (m/prefer-method! varr ::venues.namespaced.category-keyword ::categories.namespaced.category-keyword))

(doto ::venues.namespaced.with-category
  (derive ::venues.namespaced.category-keyword)
  (derive ::categories.namespaced.category-keyword))

(m/defmethod model/table-name ::venues.namespaced.with-category
  [_model]
  (model/table-name ::test/venues))

(m/defmethod model/model->namespace ::venues.namespaced.with-category
  [_model]
  {::venues.namespaced.category-keyword     :venue
   ::categories.namespaced.category-keyword :category})

(deftest ^:parallel namespaced-test
  (is (= {:select    [:*]
          :from      [[:venues :venue]]
          :left-join [:category [:= :venue.category :category.name]]
          :order-by  [[:id :asc]]}
         (tools.compile/build
           (select/select-one ::venues.namespaced.with-category
                              {:left-join [:category [:= :venue.category :category.name]]
                               :order-by  [[:id :asc]]}))))
  (is (= (instance/instance
          ::venues.namespaced.with-category
          {:venue/id                 1
           :venue/name               "Tempest"
           :venue/category           :bar
           :venue/created-at         (LocalDateTime/parse "2017-01-01T00:00")
           :venue/updated-at         (LocalDateTime/parse "2017-01-01T00:00")
           :category/name            :bar
           :category/slug            "bar_01"
           :category/parent-category nil})
         (select/select-one ::venues.namespaced.with-category
                            {:left-join [:category [:= :venue.category :category.name]]
                             :order-by  [[:id :asc]]}))))

(deftest ^:parallel do-not-realize-entire-row-test
  (testing "transformed should only need to realize the columns that get transformed"
    (test.track-realized/with-realized-columns [realized-columns]
      (is (= :bar
             (select/select-one-fn :category ::venues.category-keyword.track-realized 1)))
      (is (= #{:venues/category}
             (realized-columns))))))

(doto ::venues.category-keyword.track-realized
  (derive ::venues.category-keyword)
  (derive ::test.track-realized/venues))

(deftest ^:parallel do-not-realize-unselected-column-test
  (testing "transformed should not realize columns that are transformed if we don't actually use them in the end"
    (test.track-realized/with-realized-columns [realized-columns]
      (is (= "Tempest"
             (select/select-one-fn :name ::venues.category-keyword.track-realized 1)))
      (is (= #{:venues/name}
             (realized-columns))))))

(deftest ^:synchronized debug-logging-test
  (testing "Debug logging should not affect transforms"
    (is (= :bar
           (select/select-one-fn :category ::venues.category-keyword 1)))
    (binding [log/*level* :trace]
      (with-redefs [log/pprint-doc (constantly nil)]
        (is (= :bar
               (select/select-one-fn :category ::venues.category-keyword 1)))))))
