(ns bluejdbc.instance-test
  (:require [bluejdbc.instance :as instance]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.test :as test]
            [clojure.string :as str]
            [clojure.test :refer :all]
            [java-time :as t]
            [methodical.core :as m]))

(use-fixtures :once test/do-with-test-data)

(deftest instance-test
  (let [m (assoc (instance/instance :wow {:a 100}) :b 200)]
    (is (= {:a 100, :b 200}
           m))
    (testing "original/changes"
      (is (= {:a 100}
             (instance/original m)))
      (is (= {:b 200}
             (instance/changes m)))
      (is (= {:a 300, :b 200}
             (instance/changes (assoc m :a 300)))))
    (testing "with-original"
      (let [m2 (instance/with-original m {:a 200, :b 200})]
        (is (= {:a 100, :b 200}
               m2))
        (is (= {:a 200, :b 200}
               (instance/original m2)))
        (is (= {:a 100}
               (instance/changes m2)))))
    (testing "reset-original"
      (let [m2 (instance/reset-original m)]
        (is (= {:a 100, :b 200}
               m2))
        (is (= {:a 100, :b 200}
               (instance/original m2)))
        (is (= nil
               (instance/changes m2)))))
    (testing "table/with-table"
      (is (= :wow
             (instance/table m)))
      (is (= :ok
             (instance/table (instance/with-table m :ok)))))))

(deftest changes-test
  (is (= {:name "Hi-Dive"}
         (-> (instance/instance :venues {:id 1, :name "Tempest", :category "bar"})
             (assoc :name "Hi-Dive")
             instance/changes))))

(deftest equality-test
  (testing "equality"
    (testing "two instances with the same Table should be equal"
      (is (= (instance/instance :wow {:a 100})
             (instance/instance :wow {:a 100}))))
    (testing "Two instances with the same Connectable should be equal"
      (is (= (instance/instance :connectable nil {:a 100})
             (instance/instance :connectable nil {:a 100}))))
    (testing "Two instances with the same Connectable *and* Table should be equal"
      (is (= (instance/instance :connectable :table {:a 100})
             (instance/instance :connectable :table {:a 100}))))
    (testing "Two instances with different Tables are not equal"
      (is (not= (instance/instance :other {:a 100})
                (instance/instance :wow {:a 100}))))
    (testing "Two instances with different Connectables ARE equal"
      (is (= (instance/instance :connectable :table {:a 100})
             (instance/instance :different-connectable :table {:a 100}))))
    (testing "An Instance should be considered equal to a plain map for convenience purposes"
      (is (= {:a 100}
             (instance/instance :wow {:a 100})))
      (is (= (instance/instance :wow {:a 100})
             {:a 100})))))

(deftest instance-test-2
  (is (= {}
         (instance/instance ::MyModel)))
  (is (= {}
         (instance/instance ::MyModel {})))
  (is (= {:a 1}
         (instance/instance ::MyModel {:a 1})))
  (is (= ::MyModel
         (instance/table (instance/instance ::MyModel))))
  (is (= ::MyModel
         (instance/table (instance/instance ::MyModel {}))))
  (let [m (instance/instance ::MyModel {:original? true})]
    (is (= {:original? false}
           (assoc m :original? false)))
    (is (= {:original? true}
           (.orig m)))
    (is (= {:original? true}
           (.m m)))
    (is (= {:original? true}
           (.orig ^bluejdbc.instance.Instance (assoc m :original? false))))
    (is (= {:original? false}
           (.m ^bluejdbc.instance.Instance (assoc m :original? false))))
    (testing "fetching original value"
      (is (= {:original? true}
             (instance/original (assoc m :original? false))))
      (is (= {}
             (dissoc m :original?)))
      (is (= {:original? true}
             (instance/original (dissoc m :original?))))
      (is (= nil
             (instance/original {})))
      (is (= nil
             (instance/original nil))))))

(deftest transient-test
  (let [m  (transient (instance/instance :wow {:a 100}))
        m' (-> m
               (assoc! :b 200)
               (assoc! :c 300)
               (dissoc! :a)
               persistent!)]
    (is (= {:b 200, :c 300}
           m'))))

(deftest connectable-test
  (let [m (instance/instance :connectable :table {:a 1})]
    (is (= :connectable
           (instance/connectable m)))
    (let [m2 (instance/with-connectable m :different-connectable)]
      (testing "shouldn't modify original"
        (is (= :connectable
               (instance/connectable m))))
      (is (= :different-connectable
             (instance/connectable m2))))))

;;;; Magic Map stuff.

(deftest normalize-key-test
  (doseq [k-str  ["my-key" "my_key"]
          k-str  [k-str (str/upper-case k-str)]
          ns-str [nil "my-ns" "my_ns"]
          ns-str (if ns-str
                   [ns-str (str/upper-case ns-str)]
                   [ns-str])
          k      [k-str
                  (keyword ns-str k-str)]
          :let   [expected (cond
                             (string? k) :my-key
                             ns-str      :my-ns/my-key
                             :else       :my-key)]]
    (testing (format "%s -> %s" (pr-str k) (pr-str expected))
      (is (= expected
             (instance/normalize-key k))))))

(deftest normalize-map-test
  (testing "Should normalize keys"
    (is (= {:abc 100, :d-ef 200}
           (instance/normalize-map instance/normalize-key {:ABC 100, "dEf" 200}))))
  (testing "Should preserve metadata"
    (is (= true
           (:wow (meta (instance/normalize-map instance/normalize-key (with-meta {} {:wow true}))))))))

(deftest magic-create-test
  (let [m (instance/instance nil {:snake/snake_case 1, "SCREAMING_SNAKE_CASE" 2, :lisp-case 3, :ANGRY/LISP-CASE 4})]
    (is (= (instance/instance nil {:snake/snake-case 1, "screaming-snake-case" 2, :lisp-case 3, :angry/lisp-case 4})
           m)))
  (testing "Should be able to create a map with varargs"
    (is (= {:db-id 1}
           (instance/instance nil nil :db_id 1)))
    (is (= {:db-id 1, :table-id 2}

           (instance/instance nil nil :db_id 1, :TABLE_ID 2))))
  (testing "Should preserve metadata"
    (is (= {:x 100}
           (meta (instance/instance nil (with-meta {} {:x 100})))))))

(deftest magic-keys-test
  (testing "keys"
    (let [m (instance/instance nil {:db_id 1, :table_id 2})]
      (testing "get keys"
        (testing "get"
          (is (= 1
                 (:db_id m)))
          (is (= 1
                 (:db-id m))))
        (is (= [:db-id :table-id]
               (keys m)))
        (testing "assoc"
          (is (= (instance/instance nil {:db-id 2, :table-id 2})
                 (assoc m :db_id 2)))
          (is (= (instance/instance nil {:db-id 3, :table-id 2})
                 (assoc m :db-id 3))))
        (testing "dissoc"
          (is (= {}
                 (dissoc (instance/instance nil nil "ScReAmInG_SnAkE_cAsE" 1) "sc-re-am-in-g-sn-ak-e-c-as-e"))))
        (testing "update"
          (is (= (instance/instance nil {:db-id 2, :table-id 2})
                 (update m :db_id inc))))))))

(deftest magic-equality-test
  (testing "Two maps created with different key cases should be equal"
    (is (= (instance/instance nil {:db-id 1, :table-id 2})
           (instance/instance nil {:db_id 1, :table_id 2}))))
  (testing "should be equal to normal map with the same keys"
    (is (= {:db-id 1, :table-id 2}
           (instance/instance nil {:db_id 1, :table_id 2})))
    (is (= {}
           (instance/instance nil {})))))

(m/defmethod tableable/table-name* [:default ::venues-no-key-transform]
  [_ _ _]
  "venues")

(m/defmethod instance/key-transform-fn* [:default ::venues-no-key-transform]
  [_ _]
  identity)

(deftest no-key-xform-test
  (is (= "venues"
         (tableable/table-name ::venues-no-key-transform)))
  (test/with-default-connection
    (is (= (instance/instance :venues {:id 1, :created-at (t/local-date-time "2017-01-01T00:00")})
           (select/select-one :venues {:select [:id :created-at]})))
    (testing "Should be able to disable key transforms by overriding `key-transform-fn*`"
      (is (= {:id 1, :created_at (t/local-date-time "2017-01-01T00:00")}
             (select/select-one ::venues-no-key-transform {:select [:id :created-at]}))))))

(deftest pretty-print-test
  (testing "Should pretty-print"
    (binding [*ns* (the-ns 'bluejdbc.instance-test)]
      (is (= "(instance/instance :bluejdbc/default :venues {:id 1})"
             (pr-str (instance/instance :venues {:id 1})))))
    (testing "Should use appropriate alias for current namespace"
      (ns bluejdbc.instance-test.pretty-print-test
        (:require [bluejdbc.core :as bluejdbc]))
      (is (= "(bluejdbc/instance :bluejdbc/default :venues {:id 1})"
             (pr-str (bluejdbc.instance/instance :venues {:id 1}))))
      (binding [*ns* (the-ns 'bluejdbc.instance)]
        (is (= "(instance :bluejdbc/default :venues {:id 1})"
               (pr-str (bluejdbc.instance/instance :venues {:id 1}))))))))
