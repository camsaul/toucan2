(ns toucan2.jdbc.row-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [methodical.core :as m]
   [toucan2.core :as t2]
   [toucan2.instance :as instance]
   [toucan2.jdbc.row :as jdbc.row]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.test :as test]
   [toucan2.tools.after-select :as after-select])
  (:import
   (toucan2.jdbc.row TransientRow)))

(comment jdbc.row/keep-me)

(set! *warn-on-reflection* true)

(defn- transient-row? [row]
  (instance? TransientRow row))

(defn- do-with-row [f]
  (is (= [:ok]
         (transduce
          (map (fn [row]
                 (is (transient-row? row))
                 (f row)
                 :ok))
          conj
          (select/reducible-select ::test/venues 1)))))

(defn- realized-keys [^TransientRow row]
  @(.realized_keys row))

(defn- already-realized? [^TransientRow row]
  (realized? (.realized_row row)))

(deftest ^:parallel get-test
  (do-with-row
   (fn [row]
     (is (= "Tempest"
            (:name row)
            (get row :name)
            (get row :name ::not-found)))
     (is (= nil
            (get row :xyz)))
     (is (= ::not-found
            (get row :xyz ::not-found)))
     (is (= #{:name}
            (realized-keys row)))
     (is (not (already-realized? row))))))

(deftest ^:parallel assoc-test
  (do-with-row
   (fn [row]
     (is (transient-row? (assoc row :k 1000)))
     (is (= #{:k}
            (realized-keys row)))
     (is (contains? row :k))
     (is (not (already-realized? row)))
     (is (= 1000
            (get row :k)
            (:k row)))
     (testing "namespaced key"
       (is (transient-row? (assoc row :namespaced/k 500)))
       (is (= #{:k :namespaced/k}
              (realized-keys row)))
       (is (contains? row :k))
       (is (contains? row :namespaced/k))
       (is (not (already-realized? row)))
       (is (= 1000
              (get row :k)
              (:k row)))
       (is (= 500
              (get row :namespaced/k)
              (:namespaced/k row))))
     (doseq [v [nil false]]
       (testing (pr-str v)
         (is (transient-row? (assoc row :falsey v)))
         (is (= v
                (get row :falsey)
                (get row :falsey ::not-found)
                (:falsey row)
                (:falsey row ::not-found))))))))

(deftest ^:synchronized assoc-with-debug-logging-test
  (testing "Debug logging should not affect assoc"
    (binding [log/*level* :trace]
      (with-redefs [log/-pprint-doc (constantly nil)]
        (do-with-row
         (fn [row]
           (is (transient-row? (assoc row :k nil)))
           (is (= #{:k}
                  (realized-keys row)))
           (is (not (already-realized? row)))
           (is (= nil
                  (get row :k)
                  (get row :k ::not-found)
                  (:k row)
                  (:k row ::not-found)))))))))

(deftest ^:parallel merge-test
  (do-with-row
   (fn [row]
     (let [row' (merge row {:k 1000})]
       (doseq [row [row row']]
         (is (= #{:k}
                (realized-keys row)))
         (is (not (already-realized? row))))))))

(deftest ^:parallel contains?-test
  (do-with-row
   (fn [row]
     (is (contains? row :name))
     (is (not (contains? row :xyz)))
     (is (= #{}
            (realized-keys row)))
     (is (not (already-realized? row))))))

(deftest ^:parallel deferrable-update-test
  (testing "select just a single key"
    (do-with-row
     (fn [row]
       (let [row (protocols/deferrable-update row :name str/upper-case)]
         (is (not (already-realized? row)))
         (is (= "TEMPEST"
                (:name row)))))))
  (testing "realize the entire row without selecting the key first."
    (do-with-row
     (fn [row]
       (let [row (protocols/deferrable-update row :name str/upper-case)]
         (is (= "TEMPEST"
                (:name (realize/realize row))))))))
  (testing "add update after previously realizing the key"
    (do-with-row
     (fn [row]
       (is (= "Tempest"
              (:name row)))
       (let [row (protocols/deferrable-update row :name str/upper-case)]
         (is (= "TEMPEST"
                (:name row)))))))
  (testing "add update after previously realizing the entire row"
    (do-with-row
     (fn [row]
       (is (= "Tempest"
              (:name (realize/realize row))))
       (let [row (protocols/deferrable-update row :name str/upper-case)]
         (is (= "TEMPEST"
                (:name row))))))))

(deftest ^:parallel empty-test
  (do-with-row
   (fn [row]
     (let [empty-instance (empty row)]
       (is (instance/instance? empty-instance))
       (is (instance/instance-of? ::test/venues empty-instance))))))

(derive ::people ::test/people)

(t2/define-after-select ::people
  [person]
  (assoc person :cool-name (str "Cool " (:name person))))

(t2/define-after-select ::without-created-at
  [row]
  (let [row' (dissoc row :created-at)]
    (is (not (contains? row' :created-at)))
    (is (nil? (:created-at row')))
    row'))

(derive ::people.without-created-at ::people)
(derive ::people.without-created-at ::without-created-at)

(m/prefer-method! #'after-select/after-select
                  ::people
                  ::without-created-at)

(t2/define-after-select ::without-created-at-2
  [row]
  (let [row' (dissoc row :created-at)]
    (is (not (contains? row' :created-at)))
    (is (nil? (:created-at row')))
    (let [row'' (assoc row' :created-at 1000)]
      (is (contains? row'' :created-at))
      (is (= 1000
             (:created-at row'')))
      row'')))

(derive ::people.without-created-at-2 ::people)
(derive ::people.without-created-at-2 ::without-created-at-2)

(m/prefer-method! #'after-select/after-select
                  ::people
                  ::without-created-at-2)

(deftest ^:parallel transient-row-dissoc-test
  (testing "Dissoc should work correctly for transient rows (#105)"
    (is (= {:name       "Cam"
            :cool-name  "Cool Cam"
            :id         1
            :created-at (java.time.OffsetDateTime/parse "2020-04-21T23:56Z")}
           (t2/select-one ::people 1)))
    (is (= {:name      "Cam"
            :cool-name "Cool Cam"
            :id        1}
           (t2/select-one ::people.without-created-at 1)))
    (is (= {:name       "Cam"
            :cool-name  "Cool Cam"
            :id         1
            :created-at 1000}
           (t2/select-one ::people.without-created-at-2 1)))))
