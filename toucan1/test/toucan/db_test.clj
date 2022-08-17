(ns toucan.db-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan.db :as db]
   [toucan.test-models.address :refer [Address]]
   [toucan.test-models.category :refer [Category]]
   [toucan.test-models.heroes :as heroes]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.user :refer [User]]
   [toucan.test-setup :as test-setup]
   [toucan2.compile :as compile]
   [toucan2.connection :as conn]
   [toucan2.current :as current]
   [toucan2.instance :as instance]
   [toucan2.test :as test])
  (:import
   (java.util Locale)))

(comment test-setup/keep-me)

;;; TODO
#_(deftest override-quote-style-test
    (is (= "`toucan`"
           (binding [db/*quoting-style* :mysql]
             ((db/quote-fn) "toucan"))))
    (is (= "\"toucan\""
           (binding [db/*quoting-style* :ansi]
             ((db/quote-fn) "toucan"))))
    (is (= "[toucan]"
           (binding [db/*quoting-style* :sqlserver]
             ((db/quote-fn) "toucan")))))

(deftest dashed-field-names-test
  (testing "Test allowing dashed field names"
    (is (not (db/automatically-convert-dashes-and-underscores?)))
    (binding [db/*automatically-convert-dashes-and-underscores* true]
      (is (db/automatically-convert-dashes-and-underscores?)))
    (binding [db/*automatically-convert-dashes-and-underscores* false]
      (is (not (db/automatically-convert-dashes-and-underscores?))))
    (is (= {:street_name "1 Toucan Drive"}
           (db/select-one [Address :street_name])))
    (binding [db/*automatically-convert-dashes-and-underscores* true]
      (is (= {:street-name "1 Toucan Drive"}
             (db/select-one [Address :street-name]))))
    (binding [db/*automatically-convert-dashes-and-underscores* true]
      (is (= "1 Toucan Drive"
             (db/select-one-field :street-name Address))))))

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
           (-> (db/select-one ::UserWithMangledIdentifiers) keys set)))))

;; TODO
#_(deftest default-to-lower-case-key-xform-test
    (is (= [str/lower-case #{:first-name :last-name :id}] ; Note the absence of circumflexes over a's
           (let [original-options @@(var db/default-jdbc-options)]
             (try
               (db/set-default-jdbc-options! {:identifiers mangle-a-chars})
               ;; Setting default options without `:identifiers` should default to str/lower-case. If it doesn't, we can expect
               ;; either the current value `mangle-a-chars` (:identifiers wasn't updated), or nil (overwritten).
               (db/set-default-jdbc-options! {})
               [(:identifiers @@(var db/default-jdbc-options))
                (-> (db/select-one 'User) keys set)]
               (finally
                 (db/set-default-jdbc-options! original-options)))))))

;;; TODO
#_(deftest transaction-test
    (testing "Test transaction"
      ;; attempt to insert! two of the same Venues. Since the second has a duplicate name,
      ;; the whole transaction should fail, and neither should get inserted.
      (test/with-discarded-table-changes :venues
        (try
          (db/transaction
              (db/insert! Venue :name "Cam's Toucannery", :category "Pet Store")
              (db/insert! Venue :name "Cam's Toucannery", :category "Pet Store"))
          (catch Throwable _))
        (is (zero? (db/count Venue :name "Cam's Toucannery"))))))

(deftest with-call-counting-test
  (testing "Test with-call-counting"
    (is (= 2
           (db/with-call-counting [call-count]
             (db/select-one User)
             (db/select-one User)
             (call-count))))))

(deftest query-test
  (testing "Test query"
    (binding [current/*connectable* ::test/db]
      (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
             (db/query {:select   [:*]
                        :from     [:t1_users]
                        :order-by [:id]
                        :limit    1}))))))

(deftest lower-case-identifiers-test
  (testing "Test that identifiers are correctly lower cased in Turkish locale (toucan#59)"
    (let [original-locale (Locale/getDefault)]
      (try
        (Locale/setDefault (Locale/forLanguageTag "tr"))
        (test/create-table! ::heroes/heroes)
        (conn/with-connection [_conn ::test/db]
          (binding [compile/*honeysql-options* (assoc compile/*honeysql-options* :quoted true)]
            (let [first-row (first (db/query {:select [:ID] :from [:t1_heroes]}))]
              ;; If `db/query` (jdbc) uses [[clojure.string/lower-case]], `:ID` will be converted to `:ıd` in Turkish locale
              (is (= :id
                     (first (keys first-row)))))))
        (finally
          (Locale/setDefault original-locale))))))

(defn- transduce-to-set
  "Process `reducible-query-result` using a transducer that puts the rows from the resultset into a set"
  [reducible-query-result]
  (transduce (map identity) conj #{} reducible-query-result))

(deftest query-reducible-test
  (conn/with-connection [_conn ::test/db]
    (testing "Test query-reducible"
      (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
             (transduce-to-set (db/reducible-query {:select   [:*]
                                                    :from     [:t1_users]
                                                    :order-by [:id]
                                                    :limit    1})))))))

(deftest qualify-test
  (is (= :t1_users.first-name
         (db/qualify User :first-name)))
  (is (= :t1_users.first-name
         (db/qualify User "first-name"))))

(deftest qualified?-test
  (is (db/qualified? :users.first-name))
  (is (db/qualified? "users.first-name"))
  (is (not (db/qualified? :first-name)))
  (is (not (db/qualified? "first-name"))))

(deftest simple-select-test
  (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}]
         (db/simple-select User {:where [:= :id 1]})))
  (is (= [{:id 3, :first-name "Lucky", :last-name "Bird"}]
         (db/simple-select User {:where [:and [:not= :id 1] [:not= :id 2]]}))))

(deftest simple-select-reducible-test
  (testing "Test simple-select-reducible"
    (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}}
           (transduce-to-set (db/simple-select-reducible User {:where [:= :id 1]}))))))

;; TODO
#_(deftest test-37
    (testing "reducible-query should pass default JDBC options along to clojure.java.jdbc"
      (is (= [:connection [""] {:a 1, :b 3, :c 4}]
             (let [fn-args (atom nil)]
               (with-redefs [db/connection           (constantly :connection)
                             #_db/default-jdbc-options #_(atom {:a 1, :b 2})
                             #_jdbc/reducible-query  #_ (fn [& args]
                                                          (reset! fn-args args))]
                 (db/reducible-query {} :b 3, :c 4)))))))

(deftest simple-select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (db/simple-select-one User {:where [:= :first-name "Cam"]}))))

;; (defn do-with-default-connection [thunk]
;;   (try
;;     (m/defmethod conn/do-with-connection :toucan/default
;;       [connectable f]
;;       (next-method ::test/db f))
;;     (thunk)
;;     (finally
;;       (m/remove-primary-method! #'conn/do-with-connection :toucan/default))))

;; (defmacro with-default-connection [& body]
;;   `(do-with-default-connection (^:once fn* [] ~@body)))

(deftest update!-test
  (test/with-discarded-table-changes User
    (db/update! User 1 :last-name "Era")
    (is (= {:id 1, :first-name "Cam", :last-name "Era"}
           (db/select-one User :id 1))))
  (test/with-discarded-table-changes PhoneNumber
    (let [id "012345678"]
      (db/simple-insert! PhoneNumber {:number id, :country_code "US"})
      (db/update! PhoneNumber id :country_code "AU")
      (is (= {:number id, :country_code "AU"}
             (db/select-one PhoneNumber :number id))))))

(deftest update-where!-test
  (test/with-discarded-table-changes User
    (db/update-where! User {:first-name [:not= "Cam"]}
      :first-name "Cam")
    (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}
            {:id 2, :first-name "Cam", :last-name "Toucan"}
            {:id 3, :first-name "Cam", :last-name "Bird"}]
           (db/select User {:order-by [:id]}))))
  (test/with-discarded-table-changes User
    (db/update-where! User {:first-name "Cam"}
      :first-name "Not Cam")
    (is (= [{:id 1, :first-name "Not Cam", :last-name "Saul"}
            {:id 2, :first-name "Rasta", :last-name "Toucan"}
            {:id 3, :first-name "Lucky", :last-name "Bird"}]
           (db/select User {:order-by [:id]})))))

(deftest update-non-nil-keys!-test
  (test/with-discarded-table-changes User
    (db/update-non-nil-keys! User 2
      :first-name nil
      :last-name "Can")
    (is (= {:id 2, :first-name "Rasta", :last-name "Can"}
           (db/select-one User 2))))
  (test/with-discarded-table-changes User
    (db/update-non-nil-keys! User 2
      {:first-name nil
       :last-name  "Can"})
    (is (= {:id 2, :first-name "Rasta", :last-name "Can"}
           (db/select-one User 2)))))

(deftest simple-insert-many!-test
  (testing "It must return the inserted ids"
    (test/with-discarded-table-changes Category
      (is (= [5]
             (db/simple-insert-many! Category [{:name "seafood" :parent-category-id 100}])))))
  (testing "it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (db/simple-insert-many! User [{:first-name "Grass" :last-name (hsql/call :upper "Hopper")}
                                           {:first-name "Ko" :last-name "Libri"}]))))))

(deftest insert-many!-test
  (testing "It must return the inserted ids, it must not fail when using SQL function calls."
    (test/with-discarded-table-changes User
      (is (= [4 5]
             (db/insert-many! User [{:first-name "Grass" :last-name (hsql/call :upper "Hopper")}
                                    {:first-name "Ko" :last-name "Libri"}])))))
  (testing "It must call pre-insert hooks"
    (test/with-discarded-table-changes Category
      (is (thrown-with-msg?
           Exception
           #"A category with ID 100 does not exist"
           (db/insert-many! Category [{:name "seafood" :parent-category-id 100}]))))))

(deftest insert!-test
  (testing "It must return the inserted row"
    (test/with-discarded-table-changes User
      (is (= {:id 4, :first-name "Trash", :last-name "Bird"}
             (db/insert! User {:first-name "Trash", :last-name "Bird"}))))
    (test/with-discarded-table-changes PhoneNumber
      (is (= {:number "012345678", :country_code "AU"}
             (db/insert! PhoneNumber {:number "012345678", :country_code "AU"})))))
  (testing "The returned data must match what's been inserted in the table"
    (test/with-discarded-table-changes User
      (is (= {:id 4, :first-name "Grass", :last-name "HOPPER"}
             (db/insert! User {:first-name "Grass" :last-name (hsql/call :upper "Hopper")}))))))

#_(deftest get-inserted-id-test
  (testing "get-inserted-id shouldn't fail if nothing is returned for some reason"
    (is (= nil
           (db/get-inserted-id :id nil)))))

(deftest select-one-test
  (is (= {:id 1, :first-name "Cam", :last-name "Saul"}
         (db/select-one User, :first-name "Cam")))
  (is (= {:id 3, :first-name "Lucky", :last-name "Bird"}
         (db/select-one User {:order-by [[:id :desc]]})))
  (is (= {:first-name "Lucky", :last-name "Bird"}
         (db/select-one [User :first-name :last-name] {:order-by [[:id :desc]]}))))

(deftest select-one-field-test
  (is (= "Cam"
         (db/select-one-field :first-name User, :id 1)))
  (is (= 1
         (db/select-one-field :id User, :first-name "Cam"))))

(deftest select-one-id-test
  (is (= 1
         (db/select-one-id User, :first-name "Cam"))))

(deftest count-test
  (is (= 3
         (db/count User)))
  (is (= 1
         (db/count User, :first-name "Cam")))
  (is (= 2
         (db/count User, :first-name [:not= "Cam"]))))

(deftest select-test
  (is (= [{:id 1, :first-name "Cam", :last-name "Saul"}
          {:id 2, :first-name "Rasta", :last-name "Toucan"}
          {:id 3, :first-name "Lucky", :last-name "Bird"}]
         (db/select User {:order-by [:id]})))
  (is (= [{:id 2, :first-name "Rasta", :last-name "Toucan"}
          {:id 3, :first-name "Lucky", :last-name "Bird"}]
         (db/select User
                    :first-name [:not= "Cam"]
                    {:order-by [:id]})))
  (is (= [{:first-name "Cam", :last-name "Saul"}
          {:first-name "Rasta", :last-name "Toucan"}
          {:first-name "Lucky", :last-name "Bird"}]
         (db/select [User :first-name :last-name] {:order-by [:id]})))
  (testing "Check that `select` works as we'd expect with where clauses with more than two arguments, for example BETWEEN"
    (is (= [{:first-name "Cam", :last-name "Saul"}
            {:first-name "Rasta", :last-name "Toucan"}]
           (db/select [User :first-name :last-name] :id [:between 1 2] {:order-by [:id]})))))

(deftest select-reducible-test
  (is (= #{{:id 1, :first-name "Cam", :last-name "Saul"}
           {:id 2, :first-name "Rasta", :last-name "Toucan"}
           {:id 3, :first-name "Lucky", :last-name "Bird"}}
         (transduce-to-set (db/select-reducible User {:order-by [:id]}))))
  (testing "Add up the ids of the users in a transducer"
    (is (= 6
           (transduce (map :id) + 0 (db/select-reducible User {:order-by [:id]}))))))

(deftest select-field-test
  (is (= #{"Lucky" "Rasta" "Cam"}
         (db/select-field :first-name User)))
  (is (= #{"Lucky" "Rasta"}
         (db/select-field :first-name User, :id [:> 1])))
  (testing "Test select-ids"
    (is (= #{1 3 2}
           (db/select-ids User)))))

(deftest select-ids-test
  (is (= #{3 2}
         (db/select-ids User, :id [:not= 1]))))

(deftest select-field->field-test
  (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
         (db/select-field->field :id :first-name User)))
  (is (= {"Cam" 1, "Rasta" 2, "Lucky" 3}
         (db/select-field->field :first-name :id User)))
  (testing "Test select-id->field"
    (is (= {1 "Cam", 2 "Rasta", 3 "Lucky"}
           (db/select-id->field :first-name User)))))

(deftest exists?-test
  (is (db/exists? User, :first-name "Cam"))
  (is (db/exists? User, :first-name "Rasta", :last-name "Toucan"))
  (is (= false
         (db/exists? User, :first-name "Kanye", :last-name "Nest"))))
