(ns toucan2.core
  (:refer-clojure :exclude [count compile])
  (:require [potemkin :as p]
            [toucan2.compile :as compile]
            [toucan2.connectable :as connectable]
            [toucan2.connectable.current :as conn.current]
            [toucan2.helpers :as helpers]
            [toucan2.hydrate :as hydrate]
            [toucan2.identity-query :as identity-query]
            [toucan2.instance :as instance]
            [toucan2.log :as log]
            [toucan2.mutative :as mutative]
            [toucan2.query :as query]
            [toucan2.queryable :as queryable]
            [toucan2.realize :as realize]
            [toucan2.select :as select]
            [toucan2.tableable :as tableable]
            [toucan2.util :as u]))

(comment
  compile/keep-me
  conn.current/keep-me
  connectable/keep-me
  helpers/keep-me
  hydrate/keep-me
  identity-query/keep-me
  instance/keep-me
  log/keep-me
  mutative/keep-me
  query/keep-me
  queryable/keep-me
  realize/keep-me
  select/keep-me
  tableable/keep-me
  u/keep-me)

(p/import-vars
 [compile
  compile
  compile*]

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
  define-keys-for-automagic-hydration
  define-table-name
  deftransforms]

 [hydrate
  batched-hydrate*
  fk-keys-for-automagic-hydration*
  hydrate
  simple-hydrate*
  table-for-automagic-hydration*]

 [identity-query
  identity-query]

 [instance
  #_assoc-original
  toucan2-instance?
  changes
  instance
  instance*
  instance-of?
  key-transform-fn*
  original
  reset-original
  tableable
  update-original
  update-original-and-current]

 [log
  with-debug-logging
  with-trace-logging]

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

 [realize
  realize]

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
