(ns bluejdbc.core
  (:refer-clojure :exclude [count compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as connectable]
            [bluejdbc.connectable.current :as conn.current]
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
  conn.current/keep-me
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
  table-identifier
  to-sql*]

 [conn.current
  *current-connectable*
  *current-connection*
  default-connectable-for-tableable*
  default-options-for-connectable*
  default-options-for-tableable*]

 [connectable
  connection
  connection*
  with-connection]

 [hydrate
  batched-hydrate*
  fk-keys-for-automagic-hydration*
  hydrate
  simple-hydrate*
  table-for-automagic-hydration*]

 [instance
  changes
  instance
  instance*
  key-transform-fn*
  original
  table]

 [log
  with-debug-logging]

 [mutative
  delete!*
  delete!
  insert!*
  insert!
  insert-returning-keys!
  save!
  save!*
  update!
  update!*
  #_upsert!]

 [query
  all
  compiled
  execute!
  execute!*
  query
  query-one
  realize-row
  reducible-query
  reducible-query*
  uncompiled
  with-call-count]

 [select
  count
  count*
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
