(ns bluejdbc.core
  (:refer-clojure :exclude [count compile defmethod])
  (:require [bluejdbc.compile :as compile]
            [bluejdbc.connectable :as connectable]
            [bluejdbc.connectable.current :as conn.current]
            [bluejdbc.helpers :as helpers]
            [bluejdbc.honeysql-util :as honeysql-util]
            [bluejdbc.hydrate :as hydrate]
            [bluejdbc.instance :as instance]
            [bluejdbc.log :as log]
            [bluejdbc.mutative :as mutative]
            [bluejdbc.query :as query]
            [bluejdbc.queryable :as queryable]
            [bluejdbc.row :as row]
            [bluejdbc.select :as select]
            [bluejdbc.tableable :as tableable]
            [bluejdbc.util :as u]
            [methodical.core :as m]
            [potemkin :as p]))

(comment
  compile/keep-me
  conn.current/keep-me
  connectable/keep-me
  helpers/keep-me
  honeysql-util/keep-me
  hydrate/keep-me
  instance/keep-me
  log/keep-me
  m/keep-me
  mutative/keep-me
  query/keep-me
  queryable/keep-me
  row/keep-me
  select/keep-me
  tableable/keep-me
  u/keep-me)

(p/import-vars
 [m
  defmethod]

 [compile
  compile
  compile*
  from
  from*
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
  with-connection
  with-transaction]

 [helpers
  #_define-after-delete #_TODO
  define-after-insert
  define-after-select
  define-after-update
  define-before-delete
  define-before-insert
  define-before-select
  define-before-update
  define-hydration-keys-for-automagic-hydration
  define-table-name
  deftransforms]

 [honeysql-util
  handle-condition*
  handle-sequential-condition*]

 [hydrate
  batched-hydrate*
  fk-keys-for-automagic-hydration*
  hydrate
  simple-hydrate*
  table-for-automagic-hydration*]

 [instance
  assoc-original
  bluejdbc-instance?
  changes
  instance
  instance*
  key-transform-fn*
  original
  reset-original
  tableable]

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
  reducible-query
  reducible-query*
  uncompiled
  with-call-count]

 [queryable
  queryable
  queryable*]

 [row
  realize-row]

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
  table-name*]

 [u
  dispatch-on])
