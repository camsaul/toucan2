(ns toucan2.insert-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.connection :as conn]
   [toucan2.execute :as execute]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.pipeline :as pipeline]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.test.track-realized-columns :as test.track-realized]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.named-query :as tools.named-query])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(defrecord MyRecordType [x])

(deftest ^:parallel parse-args-test
  (testing "single map row"
    (is (= {:modelable :model, :rows [{:row 1}]}
           (query/parse-args :toucan.query-type/insert.* [:model {:row 1}]))))
  (testing "multiple map rows"
    (is (= {:modelable :model, :rows [{:row 1} {:row 2}]}
           (query/parse-args :toucan.query-type/insert.* [:model [{:row 1} {:row 2}]]))))
  (testing "kv args"
    (is (= {:modelable :model, :rows [{:a 1, :b 2, :c 3}]}
           (query/parse-args :toucan.query-type/insert.* [:model :a 1, :b 2, :c 3])))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Don't know how to interpret :toucan.query-type/insert\.\* args:"
         (query/parse-args :toucan.query-type/insert.* [:model :a 1, :b 2, :c]))))
  (testing "columns + vector rows"
    (is (= {:modelable :model
            :rows      [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}]}
           (query/parse-args :toucan.query-type/insert.* [:model [:a :b :c] [[1 2 3] [4 5 6]]])))
    (is (= {:modelable :model
            :rows      [{:name "The Ramp", :category "bar"}
                        {:name "Louie's", :category "bar"}]}
           (query/parse-args :toucan.query-type/insert.* [:model [:name :category] [["The Ramp" "bar"] ["Louie's" "bar"]]]))))
  (testing "nil"
    (is (= {:modelable :model, :rows nil}
           (query/parse-args :toucan.query-type/insert.* [:model nil]))))
  (testing "empty rows"
    (is (= {:modelable :model, :rows []}
           (query/parse-args :toucan.query-type/insert.* [:model []]))))
  (testing "queryable"
    (is (= {:modelable :model, :queryable ::named-rows}
           (query/parse-args :toucan.query-type/insert.* [:model ::named-rows]))))
  (testing "record type"
    (is (= {:modelable :model, :rows [(->MyRecordType 1)]}
           (query/parse-args :toucan.query-type/insert.* [:model (->MyRecordType 1)]))))
  (testing "positional connectable"
    (is (= {:connectable :db, :modelable :model, :rows [{:row 1}]}
           (query/parse-args :toucan.query-type/insert.* [:conn :db :model {:row 1}])))))

(deftest ^:parallel build-query-test
  (doseq [rows-fn [list vector]
          :let    [rows (rows-fn {:name "Grant & Green", :category "bar"})]]
    (testing (pr-str (list 'build :toucan.query-type/insert.* ::test/venues rows))
      (is (= {:insert-into [:venues]
              :values      [{:name "Grant & Green", :category "bar"}]}
             (pipeline/build :toucan.query-type/insert.* ::test/venues {:rows rows} {}))))))

(defn- do-insert-and-insert-returning-pks [f]
  (testing "Should be no Venue 4 yet"
    (is (= nil
           (select/select-one ::test/venues 4))))
  (doseq [[returning-keys? insert!] {false #'insert/insert!
                                     true  #'insert/insert-returning-pks!}]
    (testing (str insert!)
      (test/with-discarded-table-changes :venues
        (f returning-keys? insert!)))))

(deftest ^:synchronized insert-single-row-test
  (do-insert-and-insert-returning-pks
   (fn [returning-keys? insert!]
     (is (= (if returning-keys?
              [4]
              1)
            (insert! ::test/venues {:name "Grant & Green", :category "bar"})))
     (testing "Venue 4 should exist now"
       (is (= (instance/instance ::test/venues {:id         4
                                                :name       "Grant & Green"
                                                :category   "bar"
                                                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
              (select/select-one ::test/venues 4)))))))

(deftest ^:synchronized include-pk-test
  (testing "If a value for the PK is explicitly specified, insert! and friends should still work correctly"
    (doseq [[insert! expected] {#'insert/insert!                     1
                                #'insert/insert-returning-pks!       [4]
                                #'insert/insert-returning-instances! [(instance/instance
                                                                       ::test/venues
                                                                       {:id         4
                                                                        :name       "Grant & Green"
                                                                        :category   "bar"
                                                                        :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                                        :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]}]
      (test/with-discarded-table-changes :venues
        (testing insert!
          (is (= expected
                 (insert! ::test/venues {:id 4, :name "Grant & Green", :category "bar"})))
          (testing "Venue 4 should exist now"
            (is (= (instance/instance ::test/venues {:id         4
                                                     :name       "Grant & Green"
                                                     :category   "bar"
                                                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                   (select/select-one ::test/venues 4)))))))))

(deftest ^:synchronized include-pk-non-integer-test
  (testing "If a value for a *non-integer* PK is explicitly specified, insert! and friends should still work correctly"
    (doseq [[insert! expected] {#'insert/insert!                     1
                                #'insert/insert-returning-pks!       ["012345678"]
                                #'insert/insert-returning-instances! [(instance/instance
                                                                       ::test/phone-number
                                                                       {:number "012345678", :country-code "US"})]}]
      (test/with-discarded-table-changes :phone_number
        (testing insert!
          (is (= expected
                 (insert! ::test/phone-number {:number "012345678", :country-code "US"})))
          (testing "Phone Number 1 should exist now"
            (is (= (instance/instance ::test/phone-number {:number "012345678", :country-code "US"})
                   (select/select-one ::test/phone-number :toucan/pk "012345678")))))))))

(deftest ^:synchronized string-model-test
  (testing "insert! should work with string table names as the model"
    (conn/with-connection [_conn ::test/db]
      (do-insert-and-insert-returning-pks
       (fn [returning-keys? insert!]
         (is (= (if returning-keys?
                  [4]
                  1)
                (insert! (name (model/table-name ::test/venues)) {:name "Grant & Green", :category "bar"})))
         (testing "Venue 4 should exist now"
           (is (= (instance/instance ::test/venues {:id         4
                                                    :name       "Grant & Green"
                                                    :category   "bar"
                                                    :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                    :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                  (select/select-one ::test/venues 4)))))))))

(deftest ^:synchronized multiple-rows-test
  (doseq [rows-fn [#'list #'vector]
          :let    [rows (rows-fn
                         {:name "Black Horse London Pub", :category "bar"}
                         {:name "Nick's Crispy Tacos", :category "bar"})]]
    (testing (format "rows = %s %s\n" rows-fn (pr-str rows))
      (do-insert-and-insert-returning-pks
       (fn [returning-keys? insert!]
         (is (= (if returning-keys?
                  [4 5]
                  2)
                (insert! ::test/venues rows)))
         (testing "Venues 4 and 6 should exist now"
           (is (= [(instance/instance ::test/venues {:id         4
                                                     :name       "Black Horse London Pub"
                                                     :category   "bar"
                                                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                   (instance/instance ::test/venues {:id         5
                                                     :name       "Nick's Crispy Tacos"
                                                     :category   "bar"
                                                     :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                     :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
                  (select/select ::test/venues :id [:>= 4] {:order-by [[:id :asc]]})))))))))

(deftest ^:synchronized key-values-test
  (do-insert-and-insert-returning-pks
   (fn [returning-keys? insert!]
     (is (= (if returning-keys?
              [4]
              1)
            (insert! ::test/venues :name "HiDive SF", :category "bar")))
     (testing "Venue 4 should exist now"
       (is (= (instance/instance ::test/venues {:id         4
                                                :name       "HiDive SF"
                                                :category   "bar"
                                                :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
              (select/select-one ::test/venues :id 4)))))))

(deftest ^:synchronized multiple-rows-with-column-names-test
  (do-insert-and-insert-returning-pks
   (fn [returning-keys? insert!]
     (testing "Insert multiple rows with column names"
       (is (= (if returning-keys?
                [4 5]
                2)
              (insert! ::test/venues [:name :category] [["The Ramp" "bar"]
                                                        ["Louie's" "bar"]])))
       (testing "Venues 4 and 5 should exist now"
         (is (= [(instance/instance ::test/venues {:id 4, :name "The Ramp"})
                 (instance/instance ::test/venues {:id 5, :name "Louie's"})]
                (select/select ::test/venues :id [:> 3] {:select [:id :name], :order-by [[:id :asc]]}))))))))

(derive ::venues.composite-pk ::test/venues)

(m/defmethod model/primary-keys ::venues.composite-pk
  [_model]
  [:id :name])

(deftest ^:synchronized insert-returning-pks!-composite-pk-test
  ;; TODO FIXME -- insert-returning-pks! currently only works for Postgres, at least with the venues table that doesn't
  ;; ACTUALLY have a composite PK. H2 only returns the actual generated PK value, `:id`. Maybe we can create a new test
  ;; table or something in order to test this against H2
  (when (= (test/current-db-type) :postgres)
    (test/with-discarded-table-changes :venues
      (is (= [[4 "Grant & Green"]]
             (insert/insert-returning-pks! ::venues.composite-pk {:name "Grant & Green", :category "bar"}))))))

(deftest ^:synchronized insert!-no-changes-no-op-test
  (test/with-discarded-table-changes :venues
    (testing "If there are no rows, insert! should no-op and return zero"
      (is (= 0
             (insert/insert! ::test/venues []))))))

(deftest ^:synchronized insert-returning-instances-test
  (test/with-discarded-table-changes :venues
    (is (= [(instance/instance
             ::test/venues
             {:id         4
              :name       "Grant & Green"
              :category   "bar"
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
            (instance/instance
             ::test/venues
             {:id         5
              :name       "North Beach Cantina"
              :category   "restaurant"
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
           (insert/insert-returning-instances!
            ::test/venues
            [{:name "Grant & Green", :category "bar"}
             {:name "North Beach Cantina", :category "restaurant"}]))))
  (testing "Support wrapping the model in a vector, because why not?"
    (test/with-discarded-table-changes :venues
      (is (= [(instance/instance
               ::test/venues
               {:id   4
                :name "Grant & Green"})
              (instance/instance
               ::test/venues
               {:id   5
                :name "North Beach Cantina"})]
             (insert/insert-returning-instances!
              [::test/venues :id :name]
              [{:name "Grant & Green", :category "bar"}
               {:name "North Beach Cantina", :category "restaurant"}]))))))

(deftest ^:synchronized empty-row-test
  (testing "Should be able to insert an empty row."
    (doseq [row-or-rows [{}
                         [{}]]]
      ;; TODO -- what about multiple empty rows?? :shrug:
      (testing (format "row-or-rows = %s" (pr-str row-or-rows))
        (test/with-discarded-table-changes :birds
          (is (= [(case (test/current-db-type)
                    :h2       "INSERT INTO \"BIRDS\" DEFAULT VALUES"
                    :postgres "INSERT INTO \"birds\" DEFAULT VALUES"
                    :mariadb  "INSERT INTO `birds` () VALUES ()")]
                 (tools.compile/compile
                   (insert/insert! ::test/birds row-or-rows))))
          (is (= 1
                 (insert/insert! ::test/birds row-or-rows))))))))

(deftest ^:synchronized empty-rows-no-op-test
  (testing "insert! empty rows should no-op"
    (doseq [insert!     [#'insert/insert!
                         #'insert/insert-returning-pks!
                         #'insert/insert-returning-instances!]
            model       [::test/venues
                         :venues]
            row-or-rows [nil
                         []]]
      (testing (pr-str (list insert! model row-or-rows))
        (execute/with-call-count [call-count]
          (is (= (condp = insert!
                   #'insert/insert!                     0
                   #'insert/insert-returning-pks!       []
                   #'insert/insert-returning-instances! [])
                 (insert! model row-or-rows)))
          (testing "\ncall count"
            (is (= 0
                   (call-count)))))))))

(tools.named-query/define-named-query ::named-rows
  {:rows [{:name "Grant & Green", :category "bar"}
          {:name "North Beach Cantina", :category "restaurant"}]})

(deftest ^:synchronized named-query-test
  (doseq [insert! [#'insert/insert!
                   #'insert/insert-returning-pks!
                   #'insert/insert-returning-instances!]]
    (test/with-discarded-table-changes :venues
      (testing insert!
        (is (= (condp = insert!
                 #'insert/insert!                     2
                 #'insert/insert-returning-pks!       [4 5]
                 #'insert/insert-returning-instances! [(instance/instance
                                                        ::test/venues
                                                        {:id         4
                                                         :name       "Grant & Green"
                                                         :category   "bar"
                                                         :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                         :updated-at (LocalDateTime/parse "2017-01-01T00:00")})
                                                       (instance/instance
                                                        ::test/venues
                                                        {:id         5
                                                         :name       "North Beach Cantina"
                                                         :category   "restaurant"
                                                         :created-at (LocalDateTime/parse "2017-01-01T00:00")
                                                         :updated-at (LocalDateTime/parse "2017-01-01T00:00")})])
               (insert! ::test/venues ::named-rows)))))))

(derive ::venues.namespaced ::test/venues)

(m/defmethod model/model->namespace ::venues.namespaced
  [_model]
  {::test/venues :venue})

(deftest ^:synchronized namespaced-test
  (doseq [insert! [#'insert/insert!
                   #'insert/insert-returning-pks!
                   #'insert/insert-returning-instances!]]
    (test/with-discarded-table-changes :venues
      (testing insert!
        (is (= (condp = insert!
                 #'insert/insert!                     1
                 #'insert/insert-returning-pks!       [4]
                 #'insert/insert-returning-instances! [(instance/instance
                                                        ::venues.namespaced
                                                        {:venue/name     "Grant & Green"
                                                         :venue/category "bar"})])
               (insert! [::venues.namespaced :venue/name :venue/category]
                        {:venue/name "Grant & Green", :venue/category "bar"})))
        (is (= (instance/instance
                ::test/venues
                {:id       4
                 :name     "Grant & Green"
                 :category "bar"})
               (select/select-one [::test/venues :id :name :category] :id 4)))))))

(deftest ^:synchronized positional-connectable-test
  (testing "Support :conn positional connectable arg"
    (test/with-discarded-table-changes :venues
      (is (thrown-with-msg?
           clojure.lang.ExceptionInfo
           #"No default Toucan connection defined"
           (insert/insert! :venues {:name "Grant & Green", :category "bar"})))
      (is (= 1
             (insert/insert! :conn ::test/db :venues {:name "Grant & Green", :category "bar"})))
      (testing "nil :conn should not override current connectable"
        (binding [conn/*current-connectable* ::test/db]
          (is (= 1
                 (insert/insert! :conn nil :venues {:name "Grant & Green 2", :category "bar"})))))
      (testing "Explicit connectable should override current connectable"
        (binding [conn/*current-connectable* :fake-db]
          (is (= 1
                 (insert/insert! :conn ::test/db :venues {:name "Grant & Green 3", :category "bar"})))))
      (testing "Explicit connectable should override model default connectable"
        (is (thrown-with-msg?
             clojure.lang.ExceptionInfo
             #"Don't know how to get a connection from .* :fake-db"
             (insert/insert! :conn :fake-db ::test/venues {:name "Grant & Green", :category "bar"})))))))

(deftest ^:synchronized insert-returning-pks-should-not-realize-all-columns-test
  (testing "insert-returning-pks! should only fetch the PK column(s) when fetching results"
    (test/with-discarded-table-changes :venues
      (test.track-realized/with-realized-columns [realized-columns]
        (is (= [4]
               (insert/insert-returning-pks! ::test.track-realized/venues {:name "Walgreens", :category "store"})))
        (is (= (case (test/current-db-type)
                 (:postgres :h2) #{:venues/id}
                 :mariadb        #{:insert-id})
               (realized-columns)))))))
