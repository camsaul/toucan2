(ns bluejdbc.core
  (:refer-clojure :exclude [compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as connection]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.metadata :as metadata]
            [bluejdbc.query :as query]
            [bluejdbc.result-set :as rs]
            [bluejdbc.statement :as stmt]
            [bluejdbc.table-aware :as table-aware]
            [methodical.core :as methodical]
            [potemkin :as p]))

(comment methodical.core/keep-me
         compile/keep-me
         connection/keep-me
         hydrate/keep-me
         log/keep-me
         instance/keep-me
         metadata/keep-me
         query/keep-me
         rs/keep-me
         stmt/keep-me
         table-aware/keep-me)

;; NOCOMMIT
(defn- public-symbols [ns-alias]
  (sort (keys (ns-publics (get (ns-aliases *ns*) ns-alias)))))

(p/import-vars
 [methodical
  defmethod]

 [compile
  compile*
  compile]

 [connection
  driver
  connection*
  connection
  with-connection]

 [hydrate
  automagic-hydration-key-table
  batched-hydrate
  #_can-hydrate-with-strategy?          ; TODO <- include this?
  hydrate
  #_hydrate-with-strategy               ; TODO <- include this?
  hydration-keys
  #_hydration-strategy                  ; TODO <- include this?
  #_simple-hydrate]                     ; TODO <- include this?

 [instance
  instance
  original
  changes
  table
  with-table]

 [log
  with-debug-logging]

 [metadata
  catalogs
  columns
  database-info
  driver-info
  schemas
  table-types
  tables
  with-metadata]

 [query
  reducible-query*
  reducible-query
  query*
  query
  query-one*
  query-one
  execute!*
  execute!
  transaction]

 ;; TODO
 [rs
  maps
  read-column-thunk
  type-name]

 [stmt
  prepare!]

 [table-aware
  table-name
  primary-key*
  primary-key
  select*
  select
  select-one*
  select-one
  insert!*
  insert!
  insert-returning-keys!*
  insert-returning-keys!
  update!*
  update!
  delete!*
  delete!
  upsert!*
  upsert!
  save!*
  save!])
