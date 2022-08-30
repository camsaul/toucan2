(ns toucan2.tools.compile-test
  (:require
   [clojure.test :refer :all]
   [toucan2.delete :as delete]
   [toucan2.execute :as execute]
   [toucan2.insert :as insert]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.update :as update]))

(comment test/keep-me)

(deftest compile-test
  (execute/with-call-count [call-count]
    (is (= ["SELECT * FROM people WHERE id > ?" 1]
           (tools.compile/compile
             (select/select ::test/people :id [:> 1]))
           (tools.compile/compile
             (select/select-one ::test/people :id [:> 1]))))
    (is (= ["UPDATE venues SET name = ? WHERE id IS NULL" "Taco Bell"]
           (tools.compile/compile
             (update/update! ::test/venues nil {:name "Taco Bell"}))))
    (is (= ["DELETE FROM venues WHERE id IS NULL"]
           (tools.compile/compile
             (delete/delete! ::test/venues nil))))
    (is (= ["INSERT INTO venues (a) VALUES (?)" 1]
           (tools.compile/compile
             (insert/insert! ::test/venues {:a 1}))))
    (testing "Don't execute anything"
      (is (= 0
             (call-count))))))

(deftest build-test
  (execute/with-call-count [call-count]
    (is (= {:select [:*], :from [[:people]], :where [:> :id 1]}
           (tools.compile/build
             (select/select ::test/people :id [:> 1]))
           (tools.compile/build
             (select/select-one ::test/people :id [:> 1]))))
    (is (= {:update [:venues], :set {:name "Taco Bell"}, :where [:= :id nil]}
           (tools.compile/build
             (update/update! ::test/venues nil {:name "Taco Bell"}))))
    (is (= {:delete-from [:venues], :where [:= :id nil]}
           (tools.compile/build
             (delete/delete! ::test/venues nil))))
    (is (= {:insert-into [:venues], :values [(instance/instance ::test/venues {:a 1})]}
           (tools.compile/build
             (insert/insert! ::test/venues {:a 1}))))
    (testing "Don't execute anything"
      (is (= 0
             (call-count))))))

;;; TODO

;; (m/defmethod query/do-with-resolved-query [:default ::named-query]
;;   [_model _queryable f]
;;   (f {:select [[:count.* :count]]}))

;; (deftest resolved-test
;;   (execute/with-call-count [call-count]
;;     (is (= {:select [[:count.* :count]]}
;;            (tools.compile/resolved
;;              (select/select ::test/people :id [:> 1] ::named-query))
;;            (tools.compile/resolved
;;              (select/select-one ::test/people :id [:> 1] ::named-query))))
;;     (is (= {:update [:venues], :set {:name "Taco Bell"}, :where [:= :id nil]}
;;            (tools.compile/resolved
;;              (update/update! ::test/venues nil {:name "Taco Bell"}))))
;;     (is (= {:delete-from [:venues], :where [:= :id nil]}
;;            (tools.compile/resolved
;;              (delete/delete! ::test/venues nil))))
;;     (is (= {:insert-into [:venues], :values [(instance/instance ::test/venues {:a 1})]}
;;            (tools.compile/resolved
;;              (insert/insert! ::test/venues {:a 1}))))
;;     (testing "Don't execute anything"
;;       (is (= 0
;;              (call-count))))))
