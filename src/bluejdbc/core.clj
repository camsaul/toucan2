(ns bluejdbc.core
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as result-set]
            [bluejdbc.statement :as statement]
            [clojure.tools.logging :as log]
            [potemkin :as p]))

(comment conn/keep-me
         query/keep-me
         result-set/keep-me
         statement/keep-me)

(p/import-vars
 [conn connect! with-connection]
 [statement prepare! with-prepared-statement results]
 [result-set maps-xform namespaced-maps-xform]
 [query reducible-query query query-one execute! insert!])

;; load integrations
(doseq [[class-name integration-namespace] {"org.postgresql.Driver"   'bluejdbc.integrations.postgres
                                            "org.mariadb.jdbc.Driver" 'bluejdbc.integrations.mysql}]
  (when (try
          (Class/forName class-name)
          (catch Throwable _))
    (log/debugf "Loading integrations for %s" class-name)
    (require integration-namespace)))
