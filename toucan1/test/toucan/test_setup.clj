(ns toucan.test-setup
  "Test setup logic and helper functions. All test namespaces should require this one to make sure the env is set up
  properly."
  (:require
   [camel-snake-kebab.core :as csk]
   [clojure.java.io :as io]
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
   [toucan2.pipeline :as pipeline]
   [toucan2.test :as test]
   [toucan2.tools.update-returning-pks-workaround
    :as
    update-returning-pks-workaround]))

(set! *warn-on-reflection* true)

(t1.models/set-root-namespace! 'toucan.test-models)

(m/defmethod pipeline/transduce-query :around [:default :toucan1/model :default]
  [rf query-type model parsed-args resolved-query]
  (binding [ ;; these are also set in the wrapped test var; so these aren't strictly needed but they're set here anyway
            ;; as a REPL convenience
            update-returning-pks-workaround/*use-update-returning-pks-workaround*
            (#{:mysql :mariadb} (test/current-db-type))

            map.honeysql/*options*
            (assoc map.honeysql/*options* :dialect (test/current-honey-sql-dialect))]
    (next-method rf query-type model parsed-args resolved-query)))

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
  (next-method connectable f))

(m/defmethod test/create-table-sql-file [:default :toucan1/model]
  "For a model like `PhoneNumber`, look for either `toucan/test_models/phone_number.h2.sql` or
  `toucan/test_models/phone_number.sql`."
  [db-type model]
  (letfn [(filename [extension]
            (str "toucan/test_models/" (csk/->snake_case (name model)) extension))
          (resource-path [filename]
            (when (io/resource filename)
              filename))]
    (let [db-filename      (filename (format ".%s.sql" (name db-type)))
          generic-filename (filename ".sql")]
      (or (resource-path db-filename)
          (resource-path generic-filename)
          (throw (ex-info (format "Cannot find SQL files %s or %s"
                                  (pr-str db-filename)
                                  (pr-str generic-filename))
                          {:db-type db-type, :model model}))))))
