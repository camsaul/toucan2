(ns toucan.db-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan.db :as t1.db]
   [toucan.test-models.address :refer [Address]]
   [toucan.test-models.category :refer [Category]]
   [toucan.test-models.heroes :as heroes]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.user :refer [User]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan.test-setup :as test-setup]
   [toucan2.connection :as conn]
   [toucan2.instance :as instance]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile])
  (:import
   (java.util Locale)))

(set! *warn-on-reflection* true)

(use-fixtures :each test-setup/do-with-quoted-snake-disabled)

(comment heroes/keep-me
         test-setup/keep-me)

(deftest simple-model-test
  (testing "Simple model should use the same key transform as the original model"
    (is (identical? (instance/key-transform-fn PhoneNumber)
                    (instance/key-transform-fn (t1.db/->SimpleModel PhoneNumber))))))

;;; TODO
#_(deftest override-quote-style-test
    (is (= "`toucan`"
           (binding [t1.db/*quoting-style* :mysql]
             ((t1.db/quote-fn) "toucan"))))
    (is (= "\"toucan\""
           (binding [t1.db/*quoting-style* :ansi]
             ((t1.db/quote-fn) "toucan"))))
    (is (= "[toucan]"
           (binding [t1.db/*quoting-style* :sqlserver]
             ((t1.db/quote-fn) "toucan")))))

(deftest dashed-field-names-test
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
  (-> s name str/lower-case (str/replace "a" "â") keyword))

(m/defmethod instance/key-transform-fn ::mangled-identifiers
  [_model]
  mangle-a-chars)

(derive ::UserWithMangledIdentifiers User)
(derive ::UserWithMangledIdentifiers ::mangled-identifiers)

(deftest custom-identifiers-test
  (testing "Note the circumflexes over 'a's"
    (is (= #{:first-nâme :lâst-nâme :id}
           (-> (t1.db/select-one ::UserWithMangledIdentifiers) keys set)))))

;; TODO
#_(deftest default-to-lower-case-key-xform-test
    (is (= [str/lower-case #{:first-name :last-name :id}] ; Note the absence of circumflexes over a's
           (let [original-options @@(var t1.db/default-jdbc-options)]
             (try
               (t1.db/set-default-jdbc-options! {:identifiers mangle-a-chars})
               ;; Setting default options without `:identifiers` should default to str/lower-case. If it doesn't, we can expect
               ;; either the current value `mangle-a-chars` (:identifiers wasn't updated), or nil (overwritten).
               (t1.db/set-default-jdbc-options! {})
               [(:identifiers @@(var t1.db/default-jdbc-options))
                (-> (t1.db/select-one 'User) keys set)]
               (finally
                 (t1.db/set-default-jdbc-options! original-options)))))))

(deftest transaction-test
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

(deftest with-call-counting-test
  (testing "Test with-call-counting"
    (is (= 2
           (t1.db/with-call-counting [call-count]
             (t1.db/select-one User)
             (t1.db/select-one User)
             (call-count))))))

(deftest query-test
  (testing "Test query"
    (binding [conn/*current-connectable* ::test-setup/db]
      (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
             (t1.db/query {:select   [:*]
                        :from     [:t1_users]
                        :order-by [:id]
                        :limit    1}))))))

(deftest lower-case-identifiers-test
  ;; only test postgres, since H2 has uppercase identifiers
  (when (= (test/current-db-type) :postgres)
    (testing "Test that identifiers are correctly lower cased in Turkish locale (toucan#59)"
      (let [original-locale (Locale/getDefault)]
        (try
          (Locale/setDefault (Locale/forLanguageTag "tr"))
          (test/create-table! ::heroes/heroes)
          (conn/with-connection [_conn ::test-setup/db]
            (binding [map.honeysql/*options* (assoc map.honeysql/*options* :quoted true)]
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

(deftest query-reducible-test
  (conn/with-connection [_conn ::test-setup/db]
    (testing "Test query-reducible"
      (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
             (transduce-to-set (t1.db/reducible-query {:select   [:*]
                                                    :from     [:t1_users]
                                                    :order-by [:id]
                                                    :limit    1})))))))

(deftest qualify-test
  (is (= :t1_users.first-name
         (t1.db/qualify User :first-name)))
  (is (= :t1_users.first-name
         (t1.db/qualify User "first-name"))))

(deftest qualified?-test
  (is (t1.db/qualified? :users.first-name))
  (is (t1.db/qualified? "users.first-name"))
  (is (not (t1.db/qualified? :first-name)))
  (is (not (t1.db/qualified? "first-name"))))

(deftest simple-select-test
  (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
         (t1.db/simple-select User {:where [:= :id 1]})))
  (is (= [{:id 3, :first-name "Lucky", :last-name "Bird"}]
         (t1.db/simple-select User {:where [:and [:not= :id 1] [:not= :id 2]]}))))

(deftest simple-select-reducible-test
  (doseq [model [User
                 'User]
          model [model
                 [model :id :first-name :last-name]]]
    (testing (format "model = %s" (pr-str model))
      (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
             (transduce-to-set (t1.db/simple-select-reducible model {:where [:= :id 1]})))))))

;; TODO
#_(deftest test-37
      (testing "reducible-query should pass default JDBC options along to clojure.java.jdbc"
        (is (= [:connection [""] {:a 1, :b 3, :c 4}]
               (let [fn-args (atom nil)]
                 (with-redefs [t1.db/connection             (constantly :connection)
                               #_t1.db/default-jdbc-options #_ (atom {:a 1, :b 2})
                               #_jdbc/reducible-query    #_ (fn [& args]
                                                              (reset! fn-args args))]
                   (t1.db/reducible-query {} :b 3, :c 4)))))))

(deftest simple-select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (t1.db/simple-select-one User {:where [:= :first-name "Cam"]}))))

;; (defn do-with-default-connection [thunk]
;;   (try
;;     (m/defmethod conn/do-with-connection :default
;;       [connectable f]
;;       (next-method ::test-setup/db f))
;;     (thunk)
;;     (finally
;;       (m/remove-primary-method! #'conn/do-with-connection :default))))

;; (defmacro with-default-connection [& body]
;;   `(do-with-default-connection (^:once fn* [] ~@body)))

(deftest update!-test
  (test/with-discarded-table-changes User
    (t1.db/update! User 1 :last-name "Era")
    (is (= {:id 1, :first-name "Cam", :last-name "Era"}
           (t1.db/select-one User :id 1))))
  (test/with-discarded-table-changes PhoneNumber
    (let [id "012345678"]
      (t1.db/simple-insert! PhoneNumber {:number id, :country_code "US"})
      (t1.db/update! PhoneNumber id :country_code "AU")
      (is (= {:number id, :country_code "AU"}
             (t1.db/select-one PhoneNumber :number id))))))

(deftest update-where!-test
  (test/with-discarded-table-changes User
    (t1.db/update-where! User {:first-name [:not= "Cam"]}
                         :first-name "Cam")
    (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}
            {:id 2, :first-name "Cam", :last-name "Toucan"}
            {:id 3, :first-name "Cam", :last-name "Bird"}]
           (t1.db/select User {:order-by [:id]}))))
  (test/with-discarded-table-changes User
    (t1.db/update-where! User {:first-name "Cam"}
                         :first-name "Not Cam")
    (is (= [{:id 1, :first-name "Not Cam", :last-name "Saul"}
            {:id 2, :first-name "Rasta", :last-name "Toucan"}
            {:id 3, :first-name "Lucky", :last-name "Bird"}]
           (t1.db/select User {:order-by [:id]})))))

(deftest update-non-nil-keys!-test
  (test/with-discarded-table-changes User
    (t1.db/update-non-nil-keys! User 2
                             :first-name nil
                             :last-name "Can")
    (is (= {:id 2, :first-name "Rasta", :last-name "Can"}
           (t1.db/select-one User 2))))
  (test/with-discarded-table-changes User
    (t1.db/update-non-nil-keys! User 2
                             {:first-name nil
                              :last-name  "Can"})
    (is (= {:id 2, :first-name "Rasta", :last-name "Can"}
           (t1.db/select-one User 2)))))

(deftest simple-insert-many!-test
  (testing "It must return the inserted ids"
    (test/with-discarded-table-changes Category
      (is (= [5]
             (t1.db/simple-insert-many! Category [{:name "seafood" :parent-category-id 100}])))))
  (testing "it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (t1.db/simple-insert-many! User [{:first-name "Grass" :last-name (hsql/call :upper "Hopper")}
                                           {:first-name "Ko" :last-name "Libri"}]))))))

(deftest insert-many!-test
  (testing "It must return the inserted ids, it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (t1.db/insert-many! User [{:first-name "Grass" :last-name (hsql/call :upper "Hopper")}
                                    {:first-name "Ko" :last-name "Libri"}])))))
  (testing "It must call pre-insert hooks"
    (test/with-discarded-table-changes Category
      (is (thrown-with-msg?
           Exception
           #"A category with ID 100 does not exist"
           (t1.db/insert-many! Category [{:name "seafood" :parent-category-id 100}]))))))

(deftest insert!-test
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
             (t1.db/insert! User {:first-name "Grass" :last-name (hsql/call :upper "Hopper")}))))))

(deftest select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (t1.db/select-one User, :first-name "Cam")))
  (is (= {:id 3, :first-name "Lucky", :last-name "Bird"}
         (t1.db/select-one User {:order-by [[:id :desc]]})))
  (is (= {:first-name "Lucky", :last-name "Bird"}
         (t1.db/select-one [User :first-name :last-name] {:order-by [[:id :desc]]}))))

(deftest select-one-field-test
  (is (= "Cam"
         (t1.db/select-one-field :first-name User, :id 1)))
  (is (= 1
         (t1.db/select-one-field :id User, :first-name "Cam"))))

(deftest select-one-id-test
  (is (= 1
         (t1.db/select-one-id User, :first-name "Cam"))))

(deftest count-test
  (is (= 3
         (t1.db/count User)))
  (is (= 1
         (t1.db/count User, :first-name "Cam")))
  (is (= 2
         (t1.db/count User, :first-name [:not= "Cam"]))))

(deftest select-test
  (testing "identifiers should be quoted"
    (is (= [(case (test/current-db-type)
              :h2       "SELECT * FROM \"T1_USERS\" ORDER BY \"ID\" ASC"
              :postgres "SELECT * FROM \"t1_users\" ORDER BY \"id\" ASC")]
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

(deftest select-reducible-test
  (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}
           {:id 2, :first-name "Rasta", :last-name "Toucan"}
           {:id 3, :first-name "Lucky", :last-name "Bird"}}
         (transduce-to-set (t1.db/select-reducible User {:order-by [:id]}))))
  (testing "Add up the ids of the users in a transducer"
    (is (= 6
           (transduce (map :id) + 0 (t1.db/select-reducible User {:order-by [:id]}))))))

(deftest select-field-test
  (is (= #{"Lucky" "Rasta" "Cam"}
         (t1.db/select-field :first-name User)))
  (is (= #{"Lucky" "Rasta"}
         (t1.db/select-field :first-name User, :id [:> 1])))
  (testing "Test select-ids"
    (is (= #{1 3 2}
           (t1.db/select-ids User)))))

(deftest select-ids-test
  (is (= #{3 2}
         (t1.db/select-ids User, :id [:not= 1]))))

(deftest select-field->field-test
  (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
         (t1.db/select-field->field :id :first-name User)))
  (is (= {"Cam" 1, "Rasta" 2, "Lucky" 3}
         (t1.db/select-field->field :first-name :id User)))
  (testing "Test select-id->field"
    (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
           (t1.db/select-id->field :first-name User)))))

(deftest exists?-test
  (is (t1.db/exists? User, :first-name "Cam"))
  (is (t1.db/exists? User, :first-name "Rasta", :last-name "Toucan"))
  (is (= false
         (t1.db/exists? User, :first-name "Kanye", :last-name "Nest"))))

(deftest disable-db-logging-test
  (testing "This is just a dummy test to make sure the var actually exists."
    (is (= false
           t1.db/*disable-db-logging*))))
