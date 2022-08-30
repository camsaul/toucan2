(ns toucan2.tools.transformed-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.save :as save]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.identity-query :as identity-query]
   [toucan2.tools.transformed :as transformed]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(use-fixtures :each test/do-db-types-fixture)

(derive ::venues.category-keyword ::test/venues)
(derive ::venues.category-keyword ::transformed/transformed.model)

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

(defn- parse-int [^String s]
  (Integer/parseInt ^String s))

(m/defmethod transformed/transforms ::venues.string-id-and-category-keyword
  [_model]
  {:id {:in  parse-int
        :out str}})

(defmacro ^:private test-both-normal-and-magic-keys
  {:style/indent 1}
  [[category-key-binding] & body]
  `(doseq [[message# ~category-key-binding] {"normal keys" :category
                                             "magic keys" :CATEGORY}]
     (testing (str message# \newline)
       ~@body)))

(deftest select-in-test
  (test-both-normal-and-magic-keys [category-key]
    (testing "select should transform values going in"
      (testing "key-value condition"
        (is (= [{:id 1, :name "Tempest", :category :bar}
                {:id 2, :name "Ho's Tavern", :category :bar}]
               (select/select ::venues.category-keyword category-key :bar {:select   [:id :name category-key]
                                                                           :order-by [[:id :asc]]})))
        (testing "Toucan-style [f & args] condition"
          (is (= [{:id 1, :name "Tempest", :category :bar}
                  {:id 2, :name "Ho's Tavern", :category :bar}]
                 (select/select ::venues.category-keyword category-key [:in [:bar]]
                                {:select   [:id :name category-key]
                                 :order-by [[:id :asc]]})))))
      (testing "as the PK"
        (testing "(single value)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select ::venues.string-id-and-category-keyword :toucan/pk "1" {:select [:id :name category-key]}))))
        (testing "(vector of multiple values)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select ::venues.string-id-and-category-keyword :toucan/pk ["1"] {:select [:id :name category-key]}))))))))

(deftest select-out-test
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

(deftest normalize-transform-keys-test
  (testing "Should work if transform keys are defined in snake_case or whatever (for legacy compatibility purposes)"))

(deftest apply-row-transform-test
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

#_(derive ::venues.id-is-strin.string-id-and-category-keyword ::venues.string-id-and-category-keyword)

#_(def ^:dynamic ^:private *thunk-resolve-counts* nil)
#_(def ^:dynamic ^:private *col-read-counts* nil)

#_(m/defmethod rs/read-column-thunk [:default ::venues.id-is-strin.string-id-and-category-keyword :default]
    [connectable tableable rs ^java.sql.ResultSetMetaData rsmeta ^Long i options]
    (when *thunk-resolve-counts*
      (swap! *thunk-resolve-counts* update (.getColumnLabel rsmeta i) (fnil inc 0)))
    (let [thunk (next-method connectable tableable rs rsmeta i options)]
      (fn []
        (when *col-read-counts*
          (swap! *col-read-counts* update (.getColumnLabel rsmeta i) (fnil inc 0)))
        (thunk))))

#_(deftest select-out-only-transform-realized-columns-test
  (testing "only columns that get realized should get transformed; don't realize others as a side-effect"
    (testing "Realize both category and id"
      (binding [*col-read-counts*      (atom nil)
                *thunk-resolve-counts* (atom nil)]
        (is (= [(instance/instance
                 ::venues.id-is-strin.string-id-and-category-keyword
                 {:id "1", :category :bar})
                (instance/instance
                 ::venues.id-is-strin.string-id-and-category-keyword
                 {:id "2", :category :bar})]
               (select/select ::venues.id-is-strin.string-id-and-category-keyword
                              :id [:in ["1" "2"]]
                              {:select [:id :category]})))
        (is (= {"category" 1, "id" 1}
               @*thunk-resolve-counts*))
        (is (= {"category" 2, "id" 2}
               @*col-read-counts*))))
    (testing "Realize only id"
      (binding [*col-read-counts*      (atom nil)
                *thunk-resolve-counts* (atom nil)]
        (is (= #{"1" "2"}
               (select/select-pks-set ::venues.id-is-strin.string-id-and-category-keyword
                                      :id [:in ["1" "2"]]
                                      {:select [:id :category]})))
        (is (= {"id" 1}
               @*thunk-resolve-counts*))
        (is (= {"id" 2}
               @*col-read-counts*))))))

(deftest update!-test
  (test-both-normal-and-magic-keys [category-key]
    (testing "key-value conditions"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (update/update! ::venues.category-keyword category-key :bar {category-key :BAR})))
        (is (= #{["Ho's Tavern" :BAR] ["Tempest" :BAR]}
               (select/select-fn-set (juxt :name category-key) ::venues.category-keyword category-key :BAR))))
      (testing "Toucan-style [f & args] condition"
        (test/with-discarded-table-changes :venues
          (is (= 2
                 (update/update! ::venues.category-keyword category-key [:in [:bar]] {category-key :BAR})))
          (is (= #{:store :BAR}
                 (select/select-fn-set category-key ::venues.category-keyword))))))
    (testing "conditions map"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (update/update! ::venues.category-keyword {category-key :bar} {category-key :BAR})))))
    (testing "PK"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! ::venues.string-id-and-category-keyword "1" {:name "Wow"})))
        (is (= "Wow"
               (select/select-one-fn :name ::venues.category-keyword 1)))))))

(deftest save!-test
  (test-both-normal-and-magic-keys [category-key]
    (test/with-discarded-table-changes :venues
      (let [venue (select/select-one ::venues.category-keyword 1)]
        (is (= {:category :dive-bar}
               (protocols/changes (assoc venue category-key :dive-bar))))
        (is (= {:id         1
                :name       "Tempest"
                :category   :dive-bar
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse  "2017-01-01T00:00")}
               (save/save! (assoc venue category-key :dive-bar))))
        (is (= {:id         1
                :name       "Tempest"
                :category   :dive-bar
                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
               (select/select-one ::venues.category-keyword 1)))))))

(deftest insert!-test
  (doseq [insert! [#'insert/insert!
                   #'insert/insert-returning-pks!
                   #'insert/insert-returning-instances!]]
    (testing insert!
      (test-both-normal-and-magic-keys [category-key]
        (doseq [[args-description args] {"single map row"        [{:name "Hi-Dive", category-key :bar}]
                                         "multiple map rows"     [[{:name "Hi-Dive", category-key :bar}]]
                                         "kv args"               [:name "Hi-Dive", category-key :bar]
                                         "columns + vector rows" [[:name category-key] [["Hi-Dive" :bar]]]}]
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
                     (select/select-fn-set :name ::venues.category-keyword category-key :bar))))))
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
                   (insert! ::venues.string-id-and-category-keyword [{:name "Hi-Dive", category-key :bar}])))))))))

(deftest transform-insert-returning-results-without-select-test
  (testing "insert-returning-instances results should be transformed if they come directly from the DB (not via select)"
    (test/with-discarded-table-changes :venues
      (binding [pipeline/transduce-resolved-query (fn [rf _query-type _model {:keys [rows]} _resolved-query]
                                                    {:pre [(seq? rows)]}
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

(deftest delete!-test
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
  (test-both-normal-and-magic-keys [category-key]
    (testing "Delete row by key-value conditions"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (delete/delete! ::venues.category-keyword category-key :bar)))
        (is (= []
               (select/select ::venues.category-keyword category-key :bar))))
      (testing "Toucan-style fn-args vector"
        (test/with-discarded-table-changes :venues
          (is (= 2
                 (delete/delete! ::venues.category-keyword category-key [:in [:bar]])))
          (is (= []
                 (select/select ::venues.category-keyword category-key :bar))))))))

(deftest no-npes-test
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

(deftest one-way-transforms-test
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
  {:id {:in  parse-int
        :out str}})

(deftest deftransforms-test
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
(m/prefer-method! transformed/transforms ::transformed-venues-2 ::transformed-venues)

(deftest compose-deftransforms-test
  (is (= {:id {:in parse-int, :out str}}
         (transformed/transforms ::transformed-venues)))
  (is (= {:category {:in name, :out keyword}}
         (transformed/transforms ::transformed-venues-2)))
  (is (= {:id       {:in parse-int, :out str}
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

(deftest compose-deftransforms-override-test
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

(deftest namespaced-test
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
