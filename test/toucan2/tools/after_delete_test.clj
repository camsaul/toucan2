(ns toucan2.tools.after-delete-test
  (:require
   [clojure.test :refer :all]
   [toucan2.delete :as delete]
   [toucan2.instance :as instance]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-delete :as after-delete]
   [toucan2.tools.before-delete :as before-delete]
   [toucan2.tools.compile :as tools.compile]
   [toucan2.tools.default-fields :as default-fields]
   [toucan2.update :as update]))

(set! *warn-on-reflection* true)

(def ^:dynamic ^:private *calls* nil)

(derive ::venues.after-delete ::test/venues)

(after-delete/define-after-delete ::venues.after-delete
  [venue]
  (when *calls*
    (swap! *calls* conj [(select-keys venue [:id :name :category])
                         (select/exists? ::test/venues :id (:id venue))]))
  nil)

(deftest ^:synchronized after-delete-test
  (test/with-discarded-table-changes :venues
    (binding [*calls* (atom [])]
      (is (= 2
             (delete/delete! ::venues.after-delete :category "bar")))
      (testing "after-delete gets each deleted row as it was, after the delete"
        (is (= [[{:id 1, :name "Tempest", :category "bar"} false]
                [{:id 2, :name "Ho's Tavern", :category "bar"} false]]
               (sort-by (comp :id first) @*calls*))))
      (is (= [(instance/instance ::venues.after-delete {:id 3, :name "BevMo", :category "store"})]
             (select/select [::venues.after-delete :id :name :category]))))))

(deftest ^:synchronized honeysql-delete-test
  (testing "a Honey SQL DELETE map selects the rows it deletes from its table, keeping its alias (#203)"
    (doseq [query [{:delete-from [:venues :v], :where [:= :v.category "bar"]}
                   {:delete-from :venues, :where [:= :category "bar"]}]]
      (testing (pr-str query)
        (test/with-discarded-table-changes :venues
          (binding [*calls* (atom [])]
            (is (= 2
                   (delete/delete! ::venues.after-delete query)))
            (is (= [1 2]
                   (sort (map (comp :id first) @*calls*))))))))))

(deftest ^:synchronized unsupported-delete-test
  (testing "a delete whose rows can't be selected first throws before calling any method or deleting anything"
    (doseq [query ["DELETE FROM venues WHERE category = 'bar'"
                   ["DELETE FROM venues WHERE category = ?" "bar"]
                   {:delete-from :venues, :where [:= :category "bar"], :order-by [[:id :desc]], :limit 1}
                   {:delete-from [:venues :v], :join [[:people :p] [:= :p.id :v.id]], :where [:= :v.category "bar"]}
                   {:delete [:v], :from [[:venues :v] [:people :p]], :where [:= :p.id :v.id]}]]
      (testing (pr-str query)
        (test/with-discarded-table-changes :venues
          (binding [*calls* (atom [])]
            (is (thrown-with-msg?
                 clojure.lang.ExceptionInfo
                 #"Can't select the rows this delete deletes"
                 (delete/delete! ::venues.before-and-after-delete query)))
            (is (= []
                   @*calls*))
            (is (= [1 2 3]
                   (select/select-fn-vec :id ::test/venues {:order-by [[:id :asc]]})))))))))

(derive ::venues.after-delete.default-fields ::venues.after-delete)

(default-fields/define-default-fields ::venues.after-delete.default-fields
  [:id :name])

(deftest ^:synchronized default-fields-test
  (testing "after-delete gets the deleted rows as the model's select returns them, with default fields"
    (test/with-discarded-table-changes :venues
      (binding [*calls* (atom [])]
        (is (= 1
               (delete/delete! ::venues.after-delete.default-fields 1)))
        (is (= [[{:id 1, :name "Tempest"} false]]
               @*calls*))))))

(deftest ^:synchronized compile-test
  (testing "compiling a delete doesn't call after-delete"
    (binding [*calls* (atom [])]
      (is (= (tools.compile/compile
               (delete/delete! ::test/venues :category "bar"))
             (tools.compile/compile
               (delete/delete! ::venues.after-delete :category "bar"))))
      (is (= []
             @*calls*)))))

(derive ::venues.after-delete.parent ::test/venues)

(after-delete/define-after-delete ::venues.after-delete.parent
  [venue]
  (when *calls*
    (swap! *calls* conj [:parent (select-keys venue [:id :child])]))
  nil)

(derive ::venues.after-delete.child ::venues.after-delete.parent)

(after-delete/define-after-delete ::venues.after-delete.child
  [venue]
  (when *calls*
    (swap! *calls* conj [:child (select-keys venue [:id :child])]))
  (assoc venue :child true))

(deftest ^:synchronized compose-test
  (testing "a child model's after-delete runs first, and its parent's gets the child's result"
    (test/with-discarded-table-changes :venues
      (binding [*calls* (atom [])]
        (is (= 1
               (delete/delete! ::venues.after-delete.child 1)))
        (is (= [[:child {:id 1}]
                [:parent {:id 1, :child true}]]
               @*calls*))))))

(derive ::venues.after-delete-exception ::test/venues)

(after-delete/define-after-delete ::venues.after-delete-exception
  [venue]
  (when (= (:category venue) "store")
    (throw (ex-info "Don't delete a store!" {:venue venue}))))

(deftest ^:synchronized after-delete-exception-test
  (test/with-discarded-table-changes :venues
    (is (thrown-with-msg?
         clojure.lang.ExceptionInfo
         #"Don't delete a store!"
         (delete/delete! ::venues.after-delete-exception)))
    (testing "the delete is rolled back, as it is done in the same transaction"
      (is (= [1 2 3]
             (select/select-fn-vec :id ::venues.after-delete-exception {:order-by [[:id :asc]]}))))))

(derive ::venues.before-and-after-delete ::test/venues)

(before-delete/define-before-delete ::venues.before-and-after-delete
  [venue]
  (when *calls*
    (swap! *calls* conj [:before (:id venue)]))
  (update/update! ::test/venues (:id venue) {:name (str (:name venue) " (closed)")})
  nil)

(after-delete/define-after-delete ::venues.before-and-after-delete
  [venue]
  (when *calls*
    (swap! *calls* conj [:after (:id venue) (:name venue)]))
  nil)

(deftest ^:synchronized before-and-after-delete-test
  (test/with-discarded-table-changes :venues
    (binding [*calls* (atom [])]
      (is (= 1
             (delete/delete! ::venues.before-and-after-delete 3)))
      (testing "a model with both runs before-delete and then after-delete, which sees the changes before-delete made"
        (is (= [[:before 3] [:after 3 "BevMo (closed)"]]
               @*calls*))))))

(deftest ^:parallel macroexpansion-test
  (testing "define-after-delete makes the model, rather than its descendants, derive from ::after-delete"
    (is (= `(do
              (toucan2.util/unparent-descendants ::venues.after-delete ::after-delete/after-delete)
              (toucan2.util/maybe-derive ::venues.after-delete ::after-delete/after-delete))
           (take 3 (macroexpand-1 `(after-delete/define-after-delete ::venues.after-delete
                                     [~'venue]
                                     nil)))))))
