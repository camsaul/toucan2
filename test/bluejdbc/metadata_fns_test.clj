(ns bluejdbc.metadata-fns-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]))

(deftest with-metadata-test
  (jdbc/with-metadata [metadata (test/jdbc-url)]
    (is (= "bluejdbc.metadata.ProxyDatabaseMetaData"
           (some-> metadata class .getCanonicalName)))))
