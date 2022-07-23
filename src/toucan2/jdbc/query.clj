(ns toucan2.jdbc.query
  (:require
   [next.jdbc :as jdbc]
   [toucan2.current :as current]
   [toucan2.jdbc.result-set :as t2.jdbc.rs]))

(def global-options
  (atom nil))

(def ^:dynamic *options* nil)

(defn- options []
  (merge @global-options *options*))

(defn reduce-jdbc-query [^java.sql.Connection conn sql-args rf init]
  (with-open [stmt (jdbc/prepare conn sql-args (options))
              rset (.executeQuery stmt)]
    (reduce rf init (t2.jdbc.rs/reducible-result-set conn current/*model* rset))))

(defn execute-jdbc-query! [^java.sql.Connection conn sql-args]
  (let [options (options)]
    (jdbc/execute! conn sql-args options)))
