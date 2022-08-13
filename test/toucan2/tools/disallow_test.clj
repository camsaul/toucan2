(ns toucan2.tools.disallow-test
  #_(:require [clojure.test :refer :all]
            [methodical.core :as m]
            [toucan2.mutative :as mutative]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]
            [toucan2.test :as test]
            toucan2.tools.disallow))

;; (comment toucan2.tools.disallow/keep-me)

;; (use-fixtures :once test/do-with-test-data)
;; (use-fixtures :each test/do-with-default-connection)

;; (m/defmethod tableable/table-name* [:default ::venues]
;;   [_ _ _]
;;   "venues")

;; (derive ::venues-no-select ::venues)
;; (derive ::venues-no-select :toucan2/disallow-select)

;; (deftest disallow-select-test
;;   (is (thrown-with-msg?
;;        Exception
;;        #"You cannot select :toucan2.tools.disallow-test/venues-no-select"
;;        (select/select ::venues-no-select))))


;; (derive ::venues-no-delete ::venues)
;; (derive ::venues-no-delete :toucan2/disallow-delete)

;; (deftest disallow-delete-test
;;   (is (thrown-with-msg?
;;        Exception
;;        #"You cannot delete instances of :toucan2.tools.disallow-test/venues-no-delete"
;;        (mutative/delete! ::venues-no-delete))))

;; (derive ::venues-no-insert ::venues)
;; (derive ::venues-no-insert :toucan2/disallow-insert)

;; (deftest disallow-insert-test
;;   (is (thrown-with-msg?
;;        Exception
;;        #"You cannot create new instances of :toucan2.tools.disallow-test/venues-no-insert"
;;        (mutative/insert! ::venues-no-insert {:a 1, :b 2}))))

;; (derive ::venues-no-update ::venues)
;; (derive ::venues-no-update :toucan2/disallow-update)

;; (deftest disallow-update-test
;;   (is (thrown-with-msg?
;;        Exception
;;        #"You cannot update a :toucan2.tools.disallow-test/venues-no-update after it has been created"
;;        (mutative/update! ::venues-no-update 1 {:a 1, :b 2}))))
