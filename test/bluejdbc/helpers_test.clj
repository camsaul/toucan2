(ns bluejdbc.helpers-test
  (:require [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.helpers :as helpers]
            [bluejdbc.instance :as instance]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(m/defmethod tableable/table-name* [:default ::people]
  [_ _ _]
  "people")

(m/defmethod conn.current/default-connectable-for-tableable* ::people
  [_ _]
  :test/postgres)

(helpers/define-before-select ::people [query]
  (assoc query :select [:id :name]))

(helpers/define-after-select ::people [person]
  (assoc person ::after-select? true))

(deftest select-helpers-test
  (is (= (instance/instance ::people {:id                                  1
                                      :name                                "Cam"
                                      :bluejdbc.helpers-test/after-select? true})
         (select/select-one ::people 1))))
