(ns bluejdbc.core
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.statement :as statement]
            [clojure.tools.logging :as log]
            [java-time :as t]
            [potemkin :as p]))

(comment conn/keep-me
         query/keep-me
         result-set/keep-me
         statement/keep-me)

(p/import-vars
 [conn connection with-connection]
 [query reducible-query query query-one]
 [result-set maps-xform namespaced-maps-xform reducible-results]
 [statement prepared-statement])

;; load integrations
(doseq [[class-name integration-namespace] {"org.postgresql.Driver" 'bluejdbc.integrations.postgres}]
  (when (try
          (Class/forName class-name)
          (catch Throwable _))
    (log/debugf "Loading integrations for %s" class-name)
    (require integration-namespace)))

(defn y []
  (with-open [conn (connection "jdbc:postgresql://localhost:5432/harbormaster_dev" {:connection/user        "cam"
                                                                                    :connection/password    "cam"
                                                                                    :result-set/holdability :close-cursors-at-commit})
              stmt (prepared-statement conn {:select [[(t/offset-date-time "2020-04-15T07:04:02.465161Z") :my-date]]}
                                       {:honeysql/quoting :ansi, :honeysql/allow-dashed-names? true})
              rs   (.executeQuery stmt)]
    (transduce
     (maps-xform rs)
     conj
     []
     (reducible-results rs))
    #_:result-set-type/forward-only
    #_:result-set-concurrency/read-only
    #_:result-set-holdability/close-cursors-at-commit
    #_(pstmt/set-fetch-direction! :fetch-direction/forward)
    #_(pstmt/set-max-rows! 1)
    #_(pstmt/set-object! 1 (t/offset-date-time) :timestamp-with-timezone)
    ))

(defn x []
  (reduce
   conj
   (reducible-query "jdbc:postgresql://localhost:5432/harbormaster_dev"
                    {:select [:id :user-id [:created-at :created] [(t/offset-date-time "2020-04-15T07:04:02.465161Z") :my-date]]
                     :from   [:session]}
                    {:connection/user              "cam"
                     :connection/password          "cam"
                     :result-set/holdability       :close-cursors-at-commit
                     :honeysql/quoting             :ansi
                     :honeysql/allow-dashed-names? true
                     :results/xform                namespaced-maps-xform})))

(def url "jdbc:postgresql://localhost:5432/harbormaster_dev?user=cam&password=cam")

(defn z []
  (query url ["SELECT ?" (t/offset-date-time "2020-04-15T07:04:02.465161Z")]))

(defn a []
  (query-one url "SELECT now()"))

(defn b []
  (query-one url "SELECT now()" {:results/xform nil}))
