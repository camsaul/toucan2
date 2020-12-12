(ns bluejdbc.core
  (:refer-clojure :exclude [compile defmethod #_instance?])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as connection]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
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
         query/keep-me
         rs/keep-me
         stmt/keep-me
         table-aware/keep-me)

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
  ;; TODO -- need to import other stuff
  hydrate]

 [instance
  instance
  #_instance?
  original
  changes
  table
  with-table]

 [log
  with-debug-logging]

 ;; TODO
 #_[metadata with-metadata database-info driver-info catalogs schemas table-types tables columns]

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
 [rs read-column-thunk maps]
 [stmt prepare!]

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
