(ns toucan2.tools.transformed-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.delete :as delete]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.helpers :as helpers]
   [toucan2.tools.transformed :as transformed]
   [toucan2.update :as update])
  (:import
   (java.time LocalDateTime)))

(derive ::transformed-venues ::test/venues)
(derive ::transformed-venues ::transformed/transformed)

(m/defmethod transformed/transforms* ::transformed-venues
  [_model]
  {:category {:in  name
              :out keyword}})

(derive ::transformed-venues-id-is-string ::transformed-venues)

(defn- parse-int [^String s]
  (Integer/parseInt ^String s))

(m/defmethod transformed/transforms* ::transformed-venues-id-is-string
  [model]
  (merge
   {:id {:in  parse-int
         :out str}}
   (next-method model)))

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
               (select/select ::transformed-venues category-key :bar {:select   [:id :name category-key]
                                                                      :order-by [[:id :asc]]})))
        (testing "Toucan-style [f & args] condition"
          (is (= [{:id 1, :name "Tempest", :category :bar}
                  {:id 2, :name "Ho's Tavern", :category :bar}]
                 (select/select ::transformed-venues category-key [:in [:bar]]
                                {:select   [:id :name category-key]
                                 :order-by [[:id :asc]]})))))
      (testing "as the PK"
        (testing "(single value)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select ::transformed-venues-id-is-string :toucan/pk "1" {:select [:id :name category-key]}))))
        (testing "(vector of multiple values)"
          (is (= [{:id "1", :name "Tempest", :category :bar}]
                 (select/select ::transformed-venues-id-is-string :toucan/pk ["1"] {:select [:id :name category-key]}))))))))

(deftest select-out-test
  (testing "select should transform values coming out"
    (is (= [{:id 1, :name "Tempest", :category :bar}
            {:id 2, :name "Ho's Tavern", :category :bar}
            {:id 3, :name "BevMo", :category :store}]
           (select/select ::transformed-venues {:select [:id :name :category]})))
    (testing "should work if transformed key is not present in results"
      (is (= [{:id 1, :name "Tempest"}
              {:id 2, :name "Ho's Tavern"}
              {:id 3, :name "BevMo"}]
             (select/select ::transformed-venues {:select [:id :name]}))))
    (testing "should work with select-one and other special functions"
      (is (= :bar
             (select/select-one-fn :category ::transformed-venues :id 1)))
      (is (= #{:store :bar}
             (select/select-fn-set :category ::transformed-venues))))
    (testing "Transformed version of the map should be considered the instance 'original'"
      (let [instance (select/select-one ::transformed-venues :toucan/pk 1 {:select [:id :name :category]})]
        (is (= {:id 1, :name "Tempest", :category :bar}
               (instance/original instance)))
        (is (= nil
               (instance/changes instance)))))))

(m/defmethod transformed/transforms* ::unnormalized
  [_model]
  {:un_normalized {:in name, :out keyword}})

(deftest normalize-transform-keys-test
  (testing "Should work if transform keys are defined in snake_case or whatever (for legacy compatibility purposes)"))

;; TODO -- huh?
#_(deftest apply-row-transform-test
    (doseq [[message m] {"plain map"        {:id 1}
                         "Instance"         (instance/instance :x {:id 1})
                         ;; TODO
                         #_"custom IInstance" #_(test.custom-types/->CustomIInstance {:id 1} {:id 1})}
            [message m] (list*
                         [message m]
                         (when (instance/instance? m)
                           (for [[row-message row] {"wrapping Row"         (row/row {:id (constantly 1)})
                                                    "wrapping custom IRow" (test.custom-types/->CustomIRow
                                                                            {:id (constantly 1)})}]
                             [(str message " " row-message)
                              (-> m
                                  (instance/with-original row)
                                  (instance/with-current row))])))]
      (testing message
        (let [m2 (transformed/apply-row-transform m :id str)]
          (testing "current value"
            (is (= {:id "1"}
                   m2)))
          (when (instance/instance? m)
            (testing "original value"
              (is (= {:id "1"}
                     (instance/original m2))))
            (is (identical? (instance/current m2)
                            (instance/original m2))))))))

#_(derive ::transformed-venues-id-is-string-track-reads ::transformed-venues-id-is-string)

#_(def ^:dynamic ^:private *thunk-resolve-counts* nil)
#_(def ^:dynamic ^:private *col-read-counts* nil)

#_(m/defmethod rs/read-column-thunk [:default ::transformed-venues-id-is-string-track-reads :default]
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
                 ::transformed-venues-id-is-string-track-reads
                 {:id "1", :category :bar})
                (instance/instance
                 ::transformed-venues-id-is-string-track-reads
                 {:id "2", :category :bar})]
               (select/select ::transformed-venues-id-is-string-track-reads
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
               (select/select-pks-set ::transformed-venues-id-is-string-track-reads
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
               (update/update! ::transformed-venues category-key :bar {category-key :BAR})))
        (is (= #{["Ho's Tavern" :BAR] ["Tempest" :BAR]}
               (select/select-fn-set (juxt :name category-key) ::transformed-venues category-key :BAR))))
      (testing "Toucan-style [f & args] condition"
        (test/with-discarded-table-changes :venues
          (is (= 2
                 (update/update! ::transformed-venues category-key [:in [:bar]] {category-key :BAR})))
          (is (= #{:store :BAR}
                 (select/select-fn-set category-key ::transformed-venues))))))
    (testing "conditions map"
      (test/with-discarded-table-changes :venues
        (is (= 2
               (update/update! ::transformed-venues {category-key :bar} {category-key :BAR})))))
    (testing "PK"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (update/update! ::transformed-venues-id-is-string "1" {:name "Wow"})))
        (is (= "Wow"
               (select/select-one-fn :name ::transformed-venues 1)))))))

;; TODO
#_(deftest save!-test
    (test-both-normal-and-magic-keys [category-key]
      (test/with-discarded-table-changes :venues
        (let [venue (select/select-one ::transformed-venues 1)]
          (is (= {:id         1
                  :name       "Tempest"
                  :category   :dive-bar
                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
                  :updated-at (LocalDateTime/parse  "2017-01-01T00:00")}
                 (mutative/save! (assoc venue category-key :dive-bar))))
          (is (= {:id         1
                  :name       "Tempest"
                  :category   :dive-bar
                  :created-at (LocalDateTime/parse "2017-01-01T00:00")
                  :updated-at (LocalDateTime/parse "2017-01-01T00:00")}
                 (select/select-one ::transformed-venues 1)))))))

(deftest insert!-test
  (test-both-normal-and-magic-keys [category-key]
    (testing "single map row"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (insert/insert! ::transformed-venues {:name "Hi-Dive", category-key :bar})))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues category-key :bar)))))
    (testing "multiple map rows"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (insert/insert! ::transformed-venues [{:name "Hi-Dive", category-key :bar}])))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues category-key :bar)))))
    (testing "kv args"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (insert/insert! ::transformed-venues :name "Hi-Dive", category-key :bar)))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues category-key :bar)))))
    (testing "columns + vector rows"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (insert/insert! ::transformed-venues [:name category-key] [["Hi-Dive" :bar]])))
        (is (= #{"Tempest" "Ho's Tavern" "Hi-Dive"}
               (select/select-fn-set :name ::transformed-venues category-key :bar)))))
    (testing "returning-keys"
      (test/with-discarded-table-changes :venues
        (is (= ["4"]
               (insert/insert-returning-keys! ::transformed-venues-id-is-string [{:name "Hi-Dive", category-key "bar"}])))))))

(deftest delete!-test
    (testing "Delete row by PK"
      (test/with-discarded-table-changes :venues
        (is (= 1
               (delete/delete! ::transformed-venues-id-is-string :toucan/pk "1")))
        (is (= []
               (select/select ::test/venues 1)))
        (is (= #{2}
               (select/select-fn-set :id ::test/venues 2)))))
    (test-both-normal-and-magic-keys [category-key]
      (testing "Delete row by key-value conditions"
        (test/with-discarded-table-changes :venues
          (is (= 2
                 (delete/delete! ::transformed-venues category-key :bar)))
          (is (= []
                 (select/select ::transformed-venues category-key :bar))))
        (testing "Toucan-style fn-args vector"
          (test/with-discarded-table-changes :venues
            (is (= 2
                   (delete/delete! ::transformed-venues category-key [:in [:bar]])))
            (is (= []
                   (select/select ::transformed-venues category-key :bar))))))))

;; TODO
#_(deftest no-npes-test
    (testing "Don't apply transforms to values that are nil (avoid NPEs)"
      (testing "in"
        (testing "insert rows"
          (test/with-discarded-table-changes :venues
            ;; this should still throw an error, but it shouldn't be an NPE from the transform.
            (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo
                 #"ERROR: null value in column .* violates not-null constraint"
                 (insert/insert! ::transformed-venues {:name "No Category", :category nil})))))
        (testing "conditions"
          (is (= nil
                 (select/select-one ::transformed-venues :category nil)))))
      (testing "out"
        (let [instance (select/select-one ::transformed-venues (identity-query/identity-query
                                                                [{:id 1, :name "No Category", :category nil}]))]
          (is (= {:id 1, :name "No Category", :category nil}
                 instance))))))

(derive ::venues-transform-in-only ::test/venues)
(derive ::venues-transform-out-only ::test/venues)

;; TODO
(helpers/deftransforms ::venues-transform-in-only
  {:category {:in name}})

(helpers/deftransforms ::venues-transform-out-only
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
