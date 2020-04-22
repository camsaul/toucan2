(ns bluejdbc.core
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.options :as options]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [clojure.tools.logging :as log]
            [potemkin :as p]))

;; fool the linter/cljr-refactor
(comment conn/keep-me
         query/keep-me
         rs/keep-me
         stmt/keep-me
         options/keep-me)

(p/import-vars
 [conn connect! with-connection]
 [stmt prepare! with-prepared-statement results]
 [rs maps-xform namespaced-maps-xform]
 [query reducible-query query query-one execute! insert!]
 [options options with-options])

;; load integrations
(doseq [[class-name integration-namespace] {"org.postgresql.Driver"   'bluejdbc.integrations.postgres
                                            "org.mariadb.jdbc.Driver" 'bluejdbc.integrations.mysql}]
  (when (try
          (Class/forName class-name)
          (catch Throwable _))
    (log/debugf "Loading integrations for %s" class-name)
    (require integration-namespace)))
