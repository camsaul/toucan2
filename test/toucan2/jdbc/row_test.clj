(ns toucan2.jdbc.row-test
  (:require
   [clojure.string :as str]
   [clojure.test :refer :all]
   [toucan2.protocols :as protocols]
   [toucan2.realize :as realize]
   [toucan2.select :as select]
   [toucan2.test :as test]))

(set! *warn-on-reflection* true)

(defn- do-with-row [f]
  (is (= [:ok]
         (transduce
          (map (fn [row]
                 (is (instance? toucan2.jdbc.row.TransientRow row))
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
     (assoc row :k 1000)
     (is (= #{:k}
            (realized-keys row)))
     (is (not (already-realized? row))))))

(deftest ^:parallel merge-test
  (do-with-row
   (fn [row]
     (merge row {:k 1000})
     (is (= #{:k}
            (realized-keys row)))
     (is (not (already-realized? row))))))

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
                (:name (realize/realize row)))))))))
