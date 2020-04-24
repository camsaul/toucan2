(ns bluejdbc.metadata-test
  (:require [bluejdbc.core :as jdbc]
            [bluejdbc.test :as test]
            [clojure.test :refer :all]))

(deftest proxy-result-sets-test
  (testing "ProxyDatabaseMetaData methods should return ProxyResultSets"
    (jdbc/with-connection [conn (test/jdbc-url)]
      (let [dbmeta (.getMetaData conn)]
        (doseq [[method-name thunk] [["getAttributes" #(.getAttributes dbmeta nil nil nil nil)]
                                     #_["getBestRowIdentifier" #(.getBestRowIdentifier dbmeta nil nil nil nil false)]
                                     ["getCatalogs" #(.getCatalogs dbmeta)]
                                     ["getClientInfoProperties" #(.getClientInfoProperties dbmeta)]
                                     ["getColumnPrivileges" #(.getColumnPrivileges dbmeta nil nil nil nil)]
                                     ["getColumns" #(.getColumns dbmeta nil nil nil nil)]
                                     ["getExportedKeys" #(.getExportedKeys dbmeta nil nil nil)]
                                     ["getFunctionColumns" #(.getFunctionColumns dbmeta nil nil nil nil)]
                                     ["getFunctions" #(.getFunctions dbmeta nil nil nil)]
                                     ["getImportedKeys" #(.getImportedKeys dbmeta nil nil nil)]
                                     #_["getIndexInfo" #(.getIndexInfo dbmeta nil nil nil false false)]
                                     ["getPrimaryKeys" #(.getPrimaryKeys dbmeta nil nil nil)]
                                     ["getProcedureColumns" #(.getProcedureColumns dbmeta nil nil nil nil)]
                                     ["getProcedures" #(.getProcedures dbmeta nil nil nil)]
                                     ["getPseudoColumns" #(.getPseudoColumns dbmeta nil nil nil nil)]
                                     ["getSchemas" #(.getSchemas dbmeta)]
                                     ["getSchemas" #(.getSchemas dbmeta nil nil)]
                                     ["getSuperTables" #(.getSuperTables dbmeta nil nil nil)]
                                     ["getSuperTypes" #(.getSuperTypes dbmeta nil nil nil)]
                                     ["getTablePrivileges" #(.getTablePrivileges dbmeta nil nil nil)]
                                     ["getTableTypes" #(.getTableTypes dbmeta)]
                                     ["getTables" #(.getTables dbmeta nil nil nil nil)]
                                     ["getTypeInfo" #(.getTypeInfo dbmeta)]
                                     ["getUDTs" #(.getUDTs dbmeta nil nil nil nil)]]]
          (testing method-name
            (when-let [result (try
                                (thunk)
                                (catch java.sql.SQLFeatureNotSupportedException _
                                  nil)
                                (catch Throwable e
                                  e))]
              (is (= "bluejdbc.result_set.ProxyResultSet"
                     (if (instance? Throwable result)
                       result
                       (.getCanonicalName (class result))))))))))))
