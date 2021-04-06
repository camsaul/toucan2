(ns bluejdbc.core
  (:refer-clojure :exclude [compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connection :as connection]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.query :as query]
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
         table-aware/keep-me)

(p/import-vars
 [methodical
  defmethod]

 [compile
  compile*
  compile]

 [connection
  *connection*
  *transaction-connection*
  connectable
  connection
  connection*
  transaction
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

 [query
  *include-queries-in-exceptions*
  query*
  query
  query-all*
  query-all
  query-one*
  query-one
  execute!*
  execute!]

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
