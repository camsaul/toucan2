(ns toucan.test-setup
  "Test setup logic and helper functions. All test namespaces should require this one to make sure the env is set up
  properly."
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [honey.sql :as hsql]
   [methodical.core :as m]
   [toucan.db :as t1.db]
   [toucan.models :as t1.models]
   [toucan.test-models.address :refer [Address]]
   [toucan.test-models.category :refer [Category]]
   [toucan.test-models.phone-number :refer [PhoneNumber]]
   [toucan.test-models.user :refer [User]]
   [toucan.test-models.venue :refer [Venue]]
   [toucan2.connection :as conn]
   [toucan2.map-backend.honeysql2 :as map.honeysql]
   [toucan2.model :as model]
   [toucan2.test :as test]))

(t1.models/set-root-namespace! 'toucan.test-models)

(defn- quote-for-current-db-type [quote-fn]
  (comp (fn [s]
          ((case (test/current-db-type)
             :h2       str/upper-case
             :postgres identity #_u/lower-case-en) s))
        quote-fn))

(hsql/register-dialect!
 ::quote-for-current-db-type
 (update (hsql/get-dialect :ansi) :quote quote-for-current-db-type))

(defn do-with-default-quoting-style [thunk]
  (let [original-options @map.honeysql/global-options]
    (try
      (t1.db/set-default-quoting-style! ::quote-for-current-db-type)
      (testing (format "With default quoting style = %s\n" ::quote-for-current-db-type)
        (thunk))
      (finally
        (reset! map.honeysql/global-options original-options)))))

(defn do-with-quoted-snake-disabled [thunk]
  (binding [t1.db/*automatically-convert-dashes-and-underscores* false]
    (thunk)))

(defn- reset-db! [db-type]
  (doseq [model [Address
                 Category
                 PhoneNumber
                 User
                 Venue]]
    (test/create-table! db-type model)))

(def ^:private initialized-db-types (atom #{}))

(defn- init-db! [db-type]
  (when-not (contains? @initialized-db-types db-type)
    (reset-db! db-type)
    (swap! initialized-db-types conj db-type)))

(derive ::db ::test/db)

(m/defmethod model/default-connectable :toucan1/model
  [_model]
  ::db)

(m/defmethod conn/do-with-connection ::db
  [connectable f]
  (init-db! (test/current-db-type))
  (do-with-default-quoting-style
   (^:once fn* [] (next-method connectable f))))
