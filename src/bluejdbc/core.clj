(ns bluejdbc.core
  (:refer-clojure :exclude [count compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as connectable]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.table-aware :as table-aware]
            [bluejdbc.tableable :as tableable]
            [methodical.core :as m]
            [potemkin :as p]))

(comment
  compile/keep-me
  connectable/keep-me
  hydrate/keep-me
  instance/keep-me
  log/keep-me
  m/keep-me
  table-aware/keep-me
  tableable/keep-me)

(p/import-vars
 [m
  defmethod]

 [compile
  compile
  compile*
  from
  from*
  table-identifier]

 [connectable
  *connectable*
  *connection*
  connection
  connection*
  default-options
  with-connection]

 [hydrate
  automagic-hydration-key-table
  batched-hydrate
  can-hydrate-with-strategy?
  hydrate
  hydrate-with-strategy
  hydration-keys
  hydration-strategy
  simple-hydrate]

 [instance
  changes
  instance
  original
  table
  with-table]

 [log
  with-debug-logging]

 [table-aware
  count
  delete!
  exists?
  insert!
  insert-returning-keys!
  parse-select-args
  query-as
  save!
  select
  select*
  select-field
  select-field->field
  select-field->id
  select-ids
  select-one
  select-one-field
  select-one-id
  select-reducible
  update!
  upsert!]

 [tableable
  primary-key
  primary-key*
  table-name
  table-name*])
