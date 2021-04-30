(ns bluejdbc.core
  (:refer-clojure :exclude [count compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as connectable]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.query :as query]
            [bluejdbc.select :as select]
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
  mutative/keep-me
  query/keep-me
  select/keep-me
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
  automagic-hydration-key-table*
  batched-hydrate*
  hydrate
  simple-hydrate*]

 [instance
  changes
  instance
  original
  table]

 [log
  with-debug-logging]

 [mutative
  delete!
  insert!
  insert-returning-keys!
  save!
  save!*
  update!
  update!*
  upsert!]

 [query
  all
  execute!
  execute!*
  query
  query-one
  realize-row
  reducible-query
  reducible-query*
  with-call-count]

 [select
  count
  exists?
  reducible-query-as
  select
  select*
  select-fn->fn
  select-fn->pk
  select-fn-reducible
  select-fn-set
  select-fn-vec
  select-one
  select-one-fn
  select-one-pk
  select-pk->fn
  select-pks-reducible
  select-pks-set
  select-pks-vec
  select-reducible]

 [tableable
  primary-key*
  table-name
  table-name*])
