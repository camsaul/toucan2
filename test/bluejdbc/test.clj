(ns bluejdbc.test
  "Test utils.")

;; TODO
(defn test-dbs []
  #{:postgres})

(def ^:dynamic *db* nil)

(defn do-only [dbs thunk]
  (let [test-dbs (test-dbs)]
    (doseq [db (if (keyword? dbs)
                 [dbs]
                 (set dbs))]
      (when (contains? test-dbs db)
        (binding [*db* db]
          (thunk))))))

(defmacro only
  "Only run `body` against DBs if we are currently testing against them."
  {:style/indent 1}
  [dbs & body]
  `(do-only ~dbs (fn [] ~@body)))
