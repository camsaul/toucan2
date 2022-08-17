(ns toucan2.insert-test
  (:require
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.model :as model]
   [toucan2.query :as query]
   [toucan2.select :as select]
   [toucan2.test :as test])
  (:import
   (java.time LocalDateTime)))

(set! *warn-on-reflection* true)

(deftest parse-args-test
  (testing "single map row"
    (is (= {:rows [{:row 1}]}
           (query/parse-args ::insert/insert nil [{:row 1}]))))
  (testing "multiple map rows"
    (is (= {:rows [{:row 1} {:row 2}]}
           (query/parse-args ::insert/insert nil [[{:row 1} {:row 2}]]))))
  (testing "kv args"
    (is (= {:rows [{:a 1, :b 2, :c 3}]}
           (query/parse-args ::insert/insert nil [:a 1, :b 2, :c 3])))
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Don't know how to interpret :toucan2.insert/insert args for model nil:"
         (query/parse-args ::insert/insert nil [:a 1, :b 2, :c]))))
  (testing "columns + vector rows"
    (is (= {:rows [{:a 1, :b 2, :c 3} {:a 4, :b 5, :c 6}]}
           (query/parse-args ::insert/insert nil [[:a :b :c] [[1 2 3] [4 5 6]]])))
    (is (= {:rows [{:name "The Ramp", :category "bar"}
                   {:name "Louie's", :category "bar"}]}
           (query/parse-args ::insert/insert nil [[:name :category] [["The Ramp" "bar"] ["Louie's" "bar"]]])))))

(deftest build-query-test
  (doseq [rows-fn [list vector]
          :let    [rows (rows-fn {:name "Grant & Green", :category "bar"})]]
    (testing (pr-str (list `query/build ::insert/insert ::test/venues rows))
      (is (= {:insert-into [:venues]
              :values      [{:name "Grant & Green", :category "bar"}]}
             (query/build ::insert/insert ::test/venues {:rows rows}))))))

;;; TODO -- a bit of a misnomer now.
(defn- do-both-types-of-insert [f]
  (testing "Should be no Venue 4 yet"
    (is (= nil
           (select/select-one ::test/venues 4))))
  (doseq [[returning-keys? insert!] {false #'insert/insert!
                                     true  #'insert/insert-returning-pks!}]
    (testing (str insert!)
      (test/with-discarded-table-changes :venues
        (f returning-keys? insert!)))))

(deftest insert-single-row-test
  (do-both-types-of-insert
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

(deftest multiple-rows-test
  (doseq [rows-fn [#'list #'vector]
          :let [rows (rows-fn
                      {:name "Black Horse London Pub", :category "bar"}
                      {:name "Nick's Crispy Tacos", :category "bar"})]]
    (testing (format "rows = %s %s\n" rows-fn (pr-str rows))
      (do-both-types-of-insert
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

(deftest key-values-test
  (do-both-types-of-insert
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

(deftest multiple-rows-with-column-names-test
  (do-both-types-of-insert
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

(deftest insert-returning-pks!-composite-pk-test
  (test/with-discarded-table-changes :venues
    (is (= [[4 "Grant & Green"]]
           (insert/insert-returning-pks! ::venues.composite-pk {:name "Grant & Green", :category "bar"})))))

;;; TODO

;; (deftest insert!-custom-honeysql-test
;;   (test/with-default-connection
;;     (testing "single map row"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (insert/insert! ::venues.custom-honeysql {:id "4", :name "Hi-Dive", :category "bar"})))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one ::test/venues :id 4 {:select [:id :name]})))))
;;     (testing "multiple map rows"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (insert/insert! ::venues.custom-honeysql [{:id "4", :name "Hi-Dive", :category "bar"}])))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one ::test/venues :id 4 {:select [:id :name]})))))
;;     (testing "kv args"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (insert/insert! ::venues.custom-honeysql :id "4", :name "Hi-Dive", :category "bar")))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one ::test/venues :id 4 {:select [:id :name]})))))
;;     (testing "columns + vector rows"
;;       (test/with-discarded-table-changes :venues
;;         (is (= 1
;;                (insert/insert! ::venues.custom-honeysql [:id :name :category] [["4" "Hi-Dive" "bar"]])))
;;         (is (= {:id 4, :name "Hi-Dive"}
;;                (select/select-one ::test/venues :id 4 {:select [:id :name]})))))
;;     (testing "returning-keys"
;;       (test/with-discarded-table-changes :venues
;;         (is (= [4]
;;                (insert/insert-returning-pks! ::venues.custom-honeysql [{:id "4", :name "Hi-Dive", :category "bar"}])))))))

(deftest insert!-no-changes-no-op-test
  (test/with-discarded-table-changes :venues
    (testing "If there are no rows, insert! should no-op and return zero"
      (is (= 0
             (insert/insert! ::test/venues []))))))

(deftest insert-returning-instances-test
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
              :category   "resturaunt"
              :created-at (LocalDateTime/parse "2017-01-01T00:00")
              :updated-at (LocalDateTime/parse "2017-01-01T00:00")})]
           (insert/insert-returning-instances!
            ::test/venues
            [{:name "Grant & Green", :category "bar"}
             {:name "North Beach Cantina", :category "resturaunt"}]))))
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
               {:name "North Beach Cantina", :category "resturaunt"}]))))))
