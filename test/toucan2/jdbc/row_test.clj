(ns toucan2.jdbc.row-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.log :as log]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(defn- transient-row? [row]
  (instance? toucan2.jdbc.row.TransientRow row))

(defn- do-with-row [f]
  (is (= [:ok]
         (transduce
          (map (fn [row]
                 (is (transient-row? row))
                 (f row)
                 :ok))
          conj
          (select/reducible-select ::test/venues 1)))))

(defn- realized-keys [^toucan2.jdbc.row.TransientRow row]
  @(.realized_keys row))

(defn- already-realized? [^toucan2.jdbc.row.TransientRow row]
  @(.already_realized_QMARK_ row))

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
     (is (not (already-realized? row)))
     (is (= 1000
            (get row :k)
            (:k row)))
     (testing "namespaced key"
       (is (transient-row? (assoc row :namespaced/k 500)))
       (is (= #{:k :namespaced/k}
              (realized-keys row)))
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
      (with-redefs [log/pprint-doc (constantly nil)]
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
