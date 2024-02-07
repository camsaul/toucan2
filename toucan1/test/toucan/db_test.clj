(ns toucan.db-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan.db :as t1.db]
   [toucan.models :as t1.models]
   [toucan.test-models.address :refer [Address]]
   [toucan.test-models.category :refer [Category]]
   [toucan.test-models.heroes :as heroes]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.user :refer [User]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan.test-setup :as test-setup]
   [toucan2.connection :as conn]
   [toucan2.honeysql2 :as t2.honeysql]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.jdbc.options :as jdbc.options]
   [toucan2.jdbc.query :as jdbc.query]
   [toucan2.pipeline :as pipeline]
   [toucan2.protocols :as protocols]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.util :as u])
  (:import
   (java.time LocalDateTime)
   (java.util Locale)))

(set! *warn-on-reflection* true)

(use-fixtures :each
  test-setup/do-with-quoted-snake-disabled)

(comment heroes/keep-me
         test-setup/keep-me)

(deftest ^:parallel parse-select-options-test
  (doseq [b [:b {} 1]]
    (are [actual expected] (= expected
                              (#'t1.db/parse-select-options actual))
      [{:order-by [[:id :dec]]} :a b {:where [:= :x 1]}]
      [:a b {:where [:= :x 1], :order-by [[:id :dec]]}]

      [:a b {:order-by [[:id :dec]]} {:where [:= :x 1]}]
      [:a b {:where [:= :x 1], :order-by [[:id :dec]]}]

      [{:order-by [[:id :dec]]} {:where [:= :x 1]} :a b]
      [:a b {:where [:= :x 1], :order-by [[:id :dec]]}]

      [:a b {:order-by [[:id :dec]]} :c :d {:where [:= :x 1]}]
      [:a b :c :d {:where [:= :x 1], :order-by [[:id :dec]]}]

      [:a b {:order-by [[:id :dec]]} {:where [:= :x 1]} :c :d]
      [:a b :c :d {:where [:= :x 1], :order-by [[:id :dec]]}]

      [{:order-by [[:id :dec]]} :a b {:where [:= :x 1]} :c :d]
      [:a b :c :d {:where [:= :x 1], :order-by [[:id :dec]]}]

      [{:order-by [[:id :dec]]} {:where [:= :x 1]} :a b :c :d]
      [:a b :c :d {:where [:= :x 1], :order-by [[:id :dec]]}])))

(deftest ^:parallel totally-broken-select-test
  (testing (str "I never intentionally meant to support things like these, but as a accident of the implementation "
                "details you're actually able to stick maps wherever you want in the args\n")
    (are [f expected] (= expected
                         (f {:order-by [[:id :desc]]}
                            :category [:not= "saloon"]
                            {:where [:not= :name "BevMo"], :limit 1}))
      (partial t1.db/select Venue)                                      [{:id 2, :name "Ho's Tavern", :category :bar}]
      (partial t1.db/select-one Venue)                                  {:id 2, :name "Ho's Tavern", :category :bar}
      (partial t1.db/select-one-field :category Venue)                  :bar
      (partial t1.db/select-one-id Venue)                               2
      (partial t1.db/select-field :category Venue)                      #{:bar}
      (partial t1.db/select-ids Venue)                                  #{2}
      (partial t1.db/select-field->field :id :category Venue)           {2 :bar}
      (partial t1.db/select-id->field :category Venue)                  {2 :bar}
      (fn [& args] (into [] (apply t1.db/select-reducible Venue args))) [{:id 2, :name "Ho's Tavern", :category :bar}])
    ;; count doesn't really work with the ORDER BY, so try it with the other stuff.
    (is (= 2
           (t1.db/count Venue
                        {:limit 1}
                        :category [:not= "saloon"]
                        {:where [:not= :name "BevMo"]})))))

(deftest ^:parallel override-quote-style-test
  (is (= "`toucan`"
         (binding [t1.db/*quoting-style* :mysql]
           ((t1.db/quote-fn) "toucan"))))
  (is (= "\"toucan\""
         (binding [t1.db/*quoting-style* :ansi]
           ((t1.db/quote-fn) "toucan"))))
  (is (= "[toucan]"
         (binding [t1.db/*quoting-style* :sqlserver]
           ((t1.db/quote-fn) "toucan")))))

(deftest ^:parallel  dashed-field-names-test
  (testing "Test allowing dashed field names"
    (is (not (t1.db/automatically-convert-dashes-and-underscores?)))
    (binding [t1.db/*automatically-convert-dashes-and-underscores* true]
      (is (t1.db/automatically-convert-dashes-and-underscores?)))
    (binding [t1.db/*automatically-convert-dashes-and-underscores* false]
      (is (not (t1.db/automatically-convert-dashes-and-underscores?))))
    (is (= {:street_name "1 Toucan Drive"}
           (t1.db/select-one [Address :street_name])))
    (binding [t1.db/*automatically-convert-dashes-and-underscores* true]
      (is (= {:street-name "1 Toucan Drive"}
             (t1.db/select-one [Address :street-name]))))
    (binding [t1.db/*automatically-convert-dashes-and-underscores* true]
      (is (= "1 Toucan Drive"
             (t1.db/select-one-field :street-name Address))))))

(defn- mangle-a-chars
  [s]
  (-> s name u/lower-case-en (str/replace "a" "â") keyword))

(derive ::UserWithMangledIdentifiers User)
(derive ::UserWithMangledIdentifiers ::mangled-identifiers)

(m/defmethod pipeline/transduce-query [:default ::mangled-identifiers :default]
  [rf query-type model parsed-args resolved-query]
  (binding [jdbc.options/*options* (assoc jdbc.options/*options* :label-fn mangle-a-chars)]
    (next-method rf query-type model parsed-args resolved-query)))

(m/prefer-method! #'pipeline/transduce-query
                  [:default ::mangled-identifiers :default]
                  [:default :toucan1/model :default])

(deftest ^:parallel custom-identifiers-test
  (testing "Note the circumflexes over 'a's"
    (is (= #{:first-nâme :lâst-nâme :id}
           (-> (t1.db/select-one ::UserWithMangledIdentifiers) keys set)))))

(deftest ^:synchronized default-to-lower-case-key-xform-test
  (let [original-options @jdbc.options/global-options]
    (try
      (t1.db/set-default-jdbc-options! {:identifiers mangle-a-chars})
      (testing "Setting default options without `:identifiers` should default to str/lower-case. "
        (is (= {:label-fn u/lower-case-en}
               (select-keys (t1.db/set-default-jdbc-options! {}) [:label-fn]))))
      (testing "Note the absence of circumflexes over a's"
        (is (= #{:first-name :last-name :id}
               (-> (t1.db/select-one 'User) keys set))))
      (finally
        (reset! jdbc.options/global-options original-options)))))

(deftest ^:synchronized transaction-test
  (testing "Test transaction"
    ;; attempt to insert! two of the same Venues. Since the second has a duplicate name,
    ;; the whole transaction should fail, and neither should get inserted.
    (test/with-discarded-table-changes :venues
      (try
        (t1.db/transaction
         (t1.db/insert! Venue :name "Cam's Toucannery", :category "Pet Store")
         (t1.db/insert! Venue :name "Cam's Toucannery", :category "Pet Store"))
        (catch Throwable _))
      (is (zero? (t1.db/count Venue :name "Cam's Toucannery"))))))

(deftest ^:parallel with-call-counting-test
  (testing "Test with-call-counting"
    (is (= 2
           (t1.db/with-call-counting [call-count]
             (t1.db/select-one User)
             (t1.db/select-one User)
             (call-count))))))

(deftest ^:parallel query-test
  (testing "Test query"
    (binding [conn/*current-connectable* ::test-setup/db]
      (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
             (t1.db/query {:select   [:*]
                           :from     [:t1_users]
                           :order-by [:id]
                           :limit    1}))))))

(deftest ^:synchronized lower-case-identifiers-test
  ;; don't test H2, since it has uppercase identifiers
  (when-not (= (test/current-db-type) :h2)
    (testing "Test that identifiers are correctly lower cased in Turkish locale (toucan#59)"
      (let [original-locale (Locale/getDefault)]
        (try
          (Locale/setDefault (Locale/forLanguageTag "tr"))
          (test/create-table! ::heroes/heroes)
          (conn/with-connection [_conn ::test-setup/db]
            (binding [t2.honeysql/*options* (assoc t2.honeysql/*options* :quoted true)]
              (let [first-row (first (t1.db/query {:select [:ID] :from [:t1_heroes]}))]
                ;; If `t1.db/query` (jdbc) uses [[clojure.string/lower-case]], `:ID` will be converted to `:ıd` in Turkish locale
                (is (= :id
                       (first (keys first-row)))))))
          (finally
            (Locale/setDefault original-locale)))))))

(defn- transduce-to-set
  "Process `reducible-query-result` using a transducer that puts the rows from the resultset into a set"
  [reducible-query-result]
  (transduce (map identity) conj #{} reducible-query-result))

(deftest ^:parallel query-reducible-test
  (conn/with-connection [_conn ::test-setup/db]
    (testing "Test query-reducible"
      (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
             (transduce-to-set (t1.db/reducible-query {:select   [:*]
                                                       :from     [:t1_users]
                                                       :order-by [:id]
                                                       :limit    1})))))))

(deftest ^:parallel qualify-test
  (is (= :t1_users.first-name
         (t1.db/qualify User :first-name)))
  (is (= :t1_users.first-name
         (t1.db/qualify User "first-name"))))

(deftest ^:parallel qualified?-test
  (is (t1.db/qualified? :users.first-name))
  (is (t1.db/qualified? "users.first-name"))
  (is (not (t1.db/qualified? :first-name)))
  (is (not (t1.db/qualified? "first-name"))))

(deftest ^:parallel simple-select-test
  (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
         (t1.db/simple-select User {:where [:= :id 1]})))
  (is (= [{:id 3, :first-name "Lucky", :last-name "Bird"}]
         (t1.db/simple-select User {:where [:and [:not= :id 1] [:not= :id 2]]}))))

(deftest ^:parallel simple-select-reducible-test
  (doseq [model [User
                 'User]
          model [model
                 [model :id :first-name :last-name]]]
    (testing (format "model = %s" (pr-str model))
      (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
             (transduce-to-set (t1.db/simple-select-reducible model {:where [:= :id 1]})))))))

(deftest ^:parallel simple-select-default-fields-test
  (testing "Should not apply default fields"
    (are [thunk] (= [{:id         1
                      :name       "Tempest"
                      :category   :bar
                      :created-at (LocalDateTime/parse "2017-01-01T00:00")
                      :updated-at (LocalDateTime/parse "2017-01-01T00:00")}]
                    thunk)
      (t1.db/simple-select Venue {:where [:= :id 1]})
      (into [] (t1.db/simple-select-reducible Venue {:where [:= :id 1]})))))

(deftest ^:parallel simple-select-union-test
  (doseq [union [:union :union-all]]
    (testing union
      (let [query {union [{:select [:id :category]
                           :from   [:t1_venues]
                           :where  [:= :id 1]}
                          {:select [:id :category]
                           :from   [:t1_venues]
                           :where  [:= :id 2]}]}]
        (are [thunk] (= [{:id       1
                          :category :bar}
                         {:id       2
                          :category :bar}]
                        thunk)
          (t1.db/simple-select Venue query)
          (into [] (t1.db/simple-select-reducible Venue query)))))))

(deftest ^:synchronized reducible-query-pass-jdbc-options-test
  (testing "reducible-query should pass default JDBC options along to clojure.java.jdbc"
    (let [options (atom nil)]
      (with-redefs [jdbc.query/reduce-jdbc-query (fn [_rf _init _conn _model _sql-args extra-opts]
                                                   (reset! options (jdbc.options/merge-options extra-opts))
                                                   :ok)]
        (binding [conn/*current-connectable* ::test/db]
          (let [reducible-query (t1.db/reducible-query {} :b 3, :c 4)]
            (is (= (toucan.db/->Toucan1ReducibleQuery {} {:b 3, :c 4})
                   reducible-query))
            (is (= :ok
                   ;; This is ok because we actually want to test the `IReduce` behavior.
                   #_{:clj-kondo/ignore [:reduce-without-init]}
                   (reduce conj reducible-query)))))
        (is (= {:b 3, :c 4}
               (select-keys @options [:b :c])))))))

(deftest ^:parallel simple-select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (t1.db/simple-select-one User {:where [:= :first-name "Cam"]}))))

(deftest ^:synchronized update!-test
  (test/with-discarded-table-changes User
    (t1.db/update! User 1 :last-name "Era")
    (is (= {:id 1, :first-name "Cam", :last-name "Era"}
           (t1.db/select-one User :id 1))))
  (test/with-discarded-table-changes PhoneNumber
    (let [id "012345678"]
      (is (= "012345678"
             (t1.db/simple-insert! PhoneNumber {:number id, :country_code "US"})))
      (is (= true
             (t1.db/update! PhoneNumber id :country_code "AU")))
      (is (= {:number id, :country_code "AU"}
             (t1.db/select-one PhoneNumber :number id))))))

(deftest ^:synchronized update!-honeysql-test
  (testing "(update! model <honeysql>)  should work and skip pre-update, etc."
    (test/with-discarded-table-changes Category
      (let [built (atom [])]
        (binding [pipeline/*build* (comp (fn [result]
                                           (swap! built conj result)
                                           result)
                                         pipeline/*build*)]
          (is (= true
                 (t1.db/update! Category {:set   {:name "LIQUOR-STORE", :parent-category-id 1000}
                                          :where [:= :id 1]})))
          (testing "Built query"
            (is (= [{:update [:t1_categories]
                     :set    {:name "LIQUOR-STORE", :parent-category-id 1000}
                     :where  [:= :id 1]}]
                   @built)))
          (is (= {:id 1, :name "LIQUOR-STORE", :parent-category-id 1000}
                 ;; use simple-model to make sure transforms aren't done (str/lower-case on :name in this case)
                 (t1.db/select-one (t1.db/->SimpleModel Category) :id 1))))))))

(derive ::User.before-update User)

(t1.models/define-methods-with-IModel-method-map
 ::User.before-update
 {:pre-update (fn [user]
                (cond-> user
                  (:last-name user) (update :last-name str/upper-case)))})

(deftest ^:synchronized update-with-instance-with-no-changes-test
  (testing "update! should save the map you pass in, not just `changes`; that's what `save!` is for."
    (doseq [model [User ::User.before-update]]
      (testing model
        (test/with-discarded-table-changes User
          (let [user (-> (t1.db/select-one model :id 1)
                         (assoc :last-name "Eron")
                         instance/reset-original)]
            (is (= nil
                   (protocols/changes user)))
            (is (= true
                   (t1.db/update! model 1 user)))
            (is (= {:id 1, :first-name "Cam", :last-name (condp = model
                                                           User                 "Eron"
                                                           ::User.before-update "ERON")}
                   (t1.db/select-one [model :id :first-name :last-name] :id 1)))))))))

(derive ::Category.post-select Category)

(t1.models/define-methods-with-IModel-method-map
 ::Category.post-select
 ;; NO-OP
 {:post-select (fn [category]
                 category)})

(derive ::Category.properties Category)

(t1.models/add-property! ::parent-id
  :update (fn [category]
            (assoc category :parent-category-id 1)))

(t1.models/define-methods-with-IModel-method-map
 ::Category.properties
 ;; NO-OP
 {:properties (constantly {::parent-id true})})

(deftest ^:synchronized update!-with-types-test
  (testing "update! should work correctly for something with :types transforms and a pre-update method"
    (doseq [[message model] {""                         Category
                             "and a post-select method" ::Category.post-select
                             "and properties"           ::Category.properties}]
      (testing (format "%s\nmodel = %s" message (pr-str model))
        (test/with-discarded-table-changes Category
          (testing "before"
            (is (= {:id 1, :name "bar", :parent-category-id nil}
                   (t1.db/select-one model :id 1))))
          (is (= true
                 (t1.db/update! model 1 :name "PUB")))
          (testing "after"
            (is (= {:id 1, :name "pub", :parent-category-id (when (= model ::Category.properties)
                                                              1)}
                   (t1.db/select-one model :id 1)))))))))

(deftest ^:synchronized update-set-nil-test
  (test/with-discarded-table-changes :birds
    (is (= true
           (t1.db/update! ::test/birds 4 :bird-type "birb" :good-bird nil)))
    (is (= {:id 4, :name "Green Friend", :bird-type "birb", :good-bird nil}
           (t1.db/select-one ::test/birds :id 4)))))

(deftest ^:synchronized update-where!-test
  (testing :not=
    (doseq [model [User 'User]]
      (testing (format "model = ^%s %s" (-> model class .getCanonicalName) (pr-str model))
        (test/with-discarded-table-changes User
          (t1.db/update-where! model
                               {:first-name [:not= "Cam"]}
                               :first-name "Cam")
          (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}
                  {:id 2, :first-name "Cam", :last-name "Toucan"}
                  {:id 3, :first-name "Cam", :last-name "Bird"}]
                 (t1.db/select model {:order-by [:id]})))))))
  (testing "simple condition"
    (test/with-discarded-table-changes User
      (t1.db/update-where! User {:first-name "Cam"}
                           :first-name "Not Cam")
      (is (= [{:id 1, :first-name "Not Cam", :last-name "Saul"}
              {:id 2, :first-name "Rasta", :last-name "Toucan"}
              {:id 3, :first-name "Lucky", :last-name "Bird"}]
             (t1.db/select User {:order-by [:id]})))))
  (testing "multiple conditions"
    (test/with-discarded-table-changes User
      (t1.db/update-where! User {:first-name "Cam"
                                 :id         [:in #{1 2 3}]}
                           :first-name "Not Cam")
      (is (= [{:id 1, :first-name "Not Cam", :last-name "Saul"}
              {:id 2, :first-name "Rasta", :last-name "Toucan"}
              {:id 3, :first-name "Lucky", :last-name "Bird"}]
             (t1.db/select User {:order-by [:id]}))))))

(deftest ^:synchronized update-where!-no-pre-update-test
  (testing "update-where! should not do pre-update, transforms, etc."
    (test/with-discarded-table-changes Category
      (let [built (atom [])]
        (binding [pipeline/*build* (comp (fn [result]
                                           (swap! built conj result)
                                           result)
                                         pipeline/*build*)]
          ;; shouldn't do any validation of parent-category-id.
          (testing "Update 1: should no-op because no categories match 'BAR'"
            (is (= false
                   (t1.db/update-where! Category
                                        {:name "BAR"}
                                        {:name "LIQUOR-STORE", :parent-category-id 1000}))))
          (testing "Update 2: should change 'bar' to 'LIQUOR-STORE'"
            (is (= true
                   (t1.db/update-where! Category
                                        {:name "bar"}
                                        {:name "LIQUOR-STORE", :parent-category-id 1000}))))
          (testing "Built queries"
            (is (= [{:update [:t1_categories]
                     :set    {:name "LIQUOR-STORE", :parent-category-id 1000}
                     :where  [:= :name "BAR"]}
                    {:update [:t1_categories]
                     :set    {:name "LIQUOR-STORE", :parent-category-id 1000}
                     :where  [:= :name "bar"]}]
                   @built)))
          (is (= {:id 1, :name "LIQUOR-STORE", :parent-category-id 1000}
                 ;; use simple-model to make sure transforms aren't done (str/lower-case on :name in this case)
                 (t1.db/select-one (t1.db/->SimpleModel Category) :id 1))))))))

(deftest ^:synchronized update-where!-transaction-test
  (test/with-discarded-table-changes :venues
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"OOPS"
         (conn/with-connection [_ ::test/db]
           (t1.db/transaction
             (is (= true
                    (t1.db/update-where! ::test/venues {:id 1} :category "saloon")))
             (is (= true
                    (t1.db/update-where! ::test/venues {:id 2} {:category "saloon"})))
             (throw (ex-info "OOPS!" {}))))))
    (is (= #{"bar"}
           (t1.db/select-field :category ::test/venues :id [:in #{1 2}])))))

(deftest ^:synchronized update-non-nil-keys!-test
  (doseq [model [User ::User.before-update]
          [message thunk] {"kv-args"
                           (fn []
                             (t1.db/update-non-nil-keys! model 2
                                                         :first-name nil
                                                         :last-name "Can"))

                           "updates map"
                           (fn []
                             (t1.db/update-non-nil-keys! model 2
                                                         {:first-name nil
                                                          :last-name  "Can"}))}]
    (testing model
      (testing message
        (test/with-discarded-table-changes User
          (is (= true
                 (thunk)))
          (is (= {:id 2, :first-name "Rasta", :last-name (condp = model
                                                           User                 "Can"
                                                           ::User.before-update "CAN")}
                 (t1.db/select-one model :id 2))))))))

(deftest ^:synchronized simple-insert!-test
  (doseq [model [Category
                 'Category]]
    (testing (format "model = ^%s %s" (.getCanonicalName (class model)) (pr-str model))
      (test/with-discarded-table-changes Category
        (testing "row map"
          (is (= 5
                 (t1.db/simple-insert! model {:name "seafood" :parent-category-id 100}))))
        (testing "k v & more"
          (is (= 6
                 (t1.db/simple-insert! model :name "parrot shop" :parent-category-id 100))))))))

(deftest ^:synchronized simple-insert-many!-test
  (testing "It must return the inserted ids"
    (test/with-discarded-table-changes Category
      (is (= [5]
             (t1.db/simple-insert-many! Category [{:name "seafood" :parent-category-id 100}])))))
  (testing "it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (t1.db/simple-insert-many! User [{:first-name "Grass" :last-name [:upper "Hopper"]}
                                              {:first-name "Ko" :last-name "Libri"}]))))))

(deftest ^:synchronized insert-many!-test
  (testing "It must return the inserted ids, it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (t1.db/insert-many! User [{:first-name "Grass" :last-name [:upper "Hopper"]}
                                       {:first-name "Ko" :last-name "Libri"}])))))
  (testing "It must call pre-insert hooks"
    (test/with-discarded-table-changes Category
      (is (thrown-with-msg?
           Exception
           #"A category with ID 100 does not exist"
           (t1.db/insert-many! Category [{:name "seafood" :parent-category-id 100}]))))))

(deftest ^:synchronized insert!-test
  (testing "It must return the inserted row"
    (test/with-discarded-table-changes User
      (is (= {:id 4, :first-name "Trash", :last-name "Bird"}
             (t1.db/insert! User {:first-name "Trash", :last-name "Bird"}))))
    (test/with-discarded-table-changes PhoneNumber
      (is (= {:number "012345678", :country_code "AU"}
             (t1.db/insert! PhoneNumber {:number "012345678", :country_code "AU"})))))
  (testing "The returned data must match what's been inserted in the table"
    (test/with-discarded-table-changes User
      (is (= {:id 4, :first-name "Grass", :last-name "HOPPER"}
             (t1.db/insert! User {:first-name "Grass" :last-name [:upper "Hopper"]}))))))

(derive ::venues.edn-category ::test/venues)

(t1.models/add-type!
 ::edn
 :in  pr-str
 :out (fn [s]
        (binding [*read-eval* false]
          (read-string s))))

(t1.models/deftypes
 ::venues.edn-category
 {:category ::edn})

(deftest ^:synchronized insert!-deftypes-test
  (test/with-discarded-table-changes "venues"
    (is (= 2
           (insert/insert! ::venues.edn-category
                           [{:name "Venue 1", :category {:name "Category 1"}}
                            {:name "Venue 2", :category {:name :category-2}}])))
    (is (= [(instance/instance
             ::venues.edn-category
             {:id         5
              :name       "Venue 2"
              :category   {:name :category-2}
              :created-at (java.time.LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (java.time.LocalDateTime/parse "2017-01-01T00:00")})
            (instance/instance
             ::venues.edn-category
             {:id         4
              :name       "Venue 1"
              :category   {:name "Category 1"}
              :created-at (java.time.LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (java.time.LocalDateTime/parse "2017-01-01T00:00")})]
           (t1.db/select ::venues.edn-category {:order-by [[:id :desc]], :limit 2})))))

(deftest ^:parallel select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (t1.db/select-one User, :first-name "Cam")))
  (is (= {:id 3, :first-name "Lucky", :last-name "Bird"}
         (t1.db/select-one User {:order-by [[:id :desc]]})))
  (is (= {:first-name "Lucky", :last-name "Bird"}
         (t1.db/select-one [User :first-name :last-name] {:order-by [[:id :desc]]}))))

(deftest ^:parallel select-one-field-test
  (is (= "Cam"
         (t1.db/select-one-field :first-name User, :id 1)))
  (is (= 1
         (t1.db/select-one-field :id User, :first-name "Cam"))))

(deftest ^:parallel select-one-id-test
  (is (= 1
         (t1.db/select-one-id User, :first-name "Cam"))))

(deftest ^:parallel count-test
  (is (= 3
         (t1.db/count User)))
  (is (= 1
         (t1.db/count User, :first-name "Cam")))
  (is (= 2
         (t1.db/count User, :first-name [:not= "Cam"]))))

(deftest ^:parallel select-test
  (testing "identifiers should be quoted"
    (is (= [(case (test/current-db-type)
              :h2       "SELECT * FROM \"T1_USERS\" ORDER BY \"ID\" ASC"
              :postgres "SELECT * FROM \"t1_users\" ORDER BY \"id\" ASC"
              :mariadb  "SELECT * FROM `t1_users` ORDER BY `id` ASC")]
           (tools.compile/compile (t1.db/select User {:order-by [:id]})))))
  (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}
          {:id 2, :first-name "Rasta", :last-name "Toucan"}
          {:id 3, :first-name "Lucky", :last-name "Bird"}]
         (t1.db/select User {:order-by [:id]})))
  (is (= [{:id 2, :first-name "Rasta", :last-name "Toucan"}
          {:id 3, :first-name "Lucky", :last-name "Bird"}]
         (t1.db/select User
                       :first-name [:not= "Cam"]
                       {:order-by [:id]})))
  (is (= [{:first-name "Cam", :last-name "Saul"}
          {:first-name "Rasta", :last-name "Toucan"}
          {:first-name "Lucky", :last-name "Bird"}]
         (t1.db/select [User :first-name :last-name] {:order-by [:id]})))
  (testing "Check that `select` works as we'd expect with where clauses with more than two arguments, for example BETWEEN"
    (is (= [{:first-name "Cam", :last-name "Saul"}
            {:first-name "Rasta", :last-name "Toucan"}]
           (t1.db/select [User :first-name :last-name] :id [:between 1 2] {:order-by [:id]})))))

(deftest ^:parallel select-reducible-test
  (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}
           {:id 2, :first-name "Rasta", :last-name "Toucan"}
           {:id 3, :first-name "Lucky", :last-name "Bird"}}
         (transduce-to-set (t1.db/select-reducible User {:order-by [:id]}))))
  (testing "Add up the ids of the users in a transducer"
    (is (= 6
           (transduce (map :id) + 0 (t1.db/select-reducible User {:order-by [:id]}))))))

(deftest ^:parallel select-field-test
  (is (= #{"Lucky" "Rasta" "Cam"}
         (t1.db/select-field :first-name User)))
  (is (= #{"Lucky" "Rasta"}
         (t1.db/select-field :first-name User, :id [:> 1])))
  (testing "Test select-ids"
    (is (= #{1 3 2}
           (t1.db/select-ids User)))))

(deftest ^:parallel select-ids-test
  (is (= #{3 2}
         (t1.db/select-ids User, :id [:not= 1]))))

(deftest ^:parallel select-field->field-test
  (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
         (t1.db/select-field->field :id :first-name User)))
  (is (= {"Cam" 1, "Rasta" 2, "Lucky" 3}
         (t1.db/select-field->field :first-name :id User)))
  (testing "Test select-id->field"
    (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
           (t1.db/select-id->field :first-name User)))))

(deftest ^:parallel exists?-test
  ;; these test for `true` and `false` specifically because that's what Toucan 1 returned
  (is (= true
         (t1.db/exists? User, :first-name "Cam")))
  (is (= true
         (t1.db/exists? User, :first-name "Rasta", :last-name "Toucan")))
  (is (= false
         (t1.db/exists? User, :first-name "Kanye", :last-name "Nest")))
  (testing "with no args: just check if any rows exist"
    (is (= true
           (t1.db/exists? User))))
  (testing "apparently Toucan 1 exists? worked with a single conditions map (this was unintentional)"
    (is (= true
           (t1.db/exists? User {:first-name "Rasta", :last-name "Toucan"})))
    (is (= false
           (t1.db/exists? User {:first-name "Kanye", :last-name "Nest"})))))

(deftest ^:parallel disable-db-logging-test
  (testing "This is just a dummy test to make sure the var actually exists."
    (is (= false
           t1.db/*disable-db-logging*))))

(deftest ^:parallel honeysql->sql-test
  (is (= [(case (test/current-db-type)
            :h2       "SELECT * FROM \"USER\" WHERE \"NAME\" = ?"
            :postgres "SELECT * FROM \"user\" WHERE \"name\" = ?"
            :mariadb  "SELECT * FROM `user` WHERE `name` = ?")
          "Cam"]
         (t1.db/honeysql->sql {:select [:*], :from [:user], :where [:= :name "Cam"]}))))

(deftest ^:parallel debug-print-queries-test
  (let [s (with-out-str
            (t1.db/debug-print-queries
              (t1.db/select-one User)))]
    (is (str/includes? s "Compiling Honey SQL 2 with options"))))
