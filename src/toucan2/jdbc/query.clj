(ns toucan2.jdbc.query
  (:require [next.jdbc :as jdbc]
            [toucan2.jdbc.result-set :as t2.jdbc.rs]
            [toucan2.dynamic :as dynamic]))

(def global-options
  (atom nil))

(def ^:dynamic *options* nil)

(defn reduce-jdbc-query [^java.sql.Connection conn sql-args rf init]
  (let [options (merge @global-options *options*)]
    (with-open [stmt (jdbc/prepare conn sql-args options)
                rset (.executeQuery stmt)]
      (reduce rf init (t2.jdbc.rs/reducible-result-set conn dynamic/*model* rset)))))
