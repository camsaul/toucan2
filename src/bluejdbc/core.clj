(ns bluejdbc.core
  (:refer-clojure :exclude [defmethod type])
  (:require [bluejdbc.connection :as conn]
            [bluejdbc.high-level :as high-level]
            [bluejdbc.metadata :as metadata]
            [bluejdbc.metadata-fns :as metadata-fns]
            [bluejdbc.options :as options]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.types :as types]
            [bluejdbc.util :as u]
            [bluejdbc.util.log :as log]
            [methodical.core :as methodical]
            [potemkin :as p]))

;; fool the linter/cljr-refactor
(comment conn/keep-me
         high-level/keep-me
         metadata/keep-me
         metadata-fns/keep-me
         methodical/keep-me
         log/keep-me
         rs/keep-me
         stmt/keep-me
         types/keep-me
         options/keep-me
         u/keep-me)

(p/import-vars
 [conn connect! named-connectable transaction-isolation-level with-connection]
 [high-level execute! insert! insert-returning-keys! query query-one reducible-query transaction update! delete!]
 [metadata metadata]
 [metadata-fns with-metadata database-info driver-info catalogs schemas table-types tables columns]
 [methodical defmethod]
 [log with-debug-logging]
 [options options with-options]
 [rs maps]
 [stmt prepare! with-prepared-statement results]
 [types type]
 [u reverse-lookup])

(p/import-def rs/concurrency result-set-concurrency)
(p/import-def rs/fetch-direction result-set-fetch-direction)
(p/import-def rs/type result-set-type)
(p/import-def rs/holdability result-set-holdability)


;; load integrations
(doseq [[class-name integration-namespace] {"org.postgresql.Driver"   'bluejdbc.integrations.postgres
                                            "org.mariadb.jdbc.Driver" 'bluejdbc.integrations.mysql}]
  (when (try
          (Class/forName class-name)
          (catch Throwable _))
    (log/debugf "Loading integrations for %s" class-name)
    (require integration-namespace)))

(defn x []
  (log/tracef "TEsting %d" 100))
