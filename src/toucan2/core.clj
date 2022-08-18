(ns toucan2.core
  "Convenience namespace exposing the most common parts of the library's public API for day-to-day usage (i.e., not
  implementing anything advanced)"
  (:refer-clojure :exclude [compile count instance?])
  (:require
   [potemkin :as p]
   [toucan2.compile]
   [toucan2.connection]
   [toucan2.delete]
   [toucan2.execute]
   [toucan2.insert]
   [toucan2.instance]
   [toucan2.model]
   [toucan2.save]
   [toucan2.select]
   [toucan2.tools.after-insert]
   [toucan2.tools.after-update]
   [toucan2.tools.before-insert]
   [toucan2.tools.before-update]
   [toucan2.tools.helpers]
   [toucan2.tools.hydrate]
   [toucan2.tools.identity-query]
   [toucan2.tools.transformed]
   [toucan2.update]
   [toucan2.util]))

(comment
  toucan2.compile/keep-me
  toucan2.connection/keep-me
  toucan2.delete/keep-me
  toucan2.execute/keep-me
  toucan2.insert/keep-me
  toucan2.instance/keep-me
  toucan2.model/keep-me
  toucan2.save/keep-me
  toucan2.select/keep-me
  toucan2.tools.after-insert/keep-me
  toucan2.tools.after-update/keep-me
  toucan2.tools.before-insert/keep-me
  toucan2.tools.before-update/keep-me
  toucan2.tools.helpers/keep-me
  toucan2.tools.hydrate/keep-me
  toucan2.tools.identity-query/keep-me
  toucan2.tools.transformed/keep-me
  toucan2.update/keep-me
  toucan2.util/keep-me)

(p/import-vars
 [toucan2.compile
  *honeysql-options*
  global-honeysql-options
  with-compiled-query]

 [toucan2.connection
  do-with-connection
  with-connection
  with-transaction]

 [toucan2.delete
  delete!]

 [toucan2.execute
  compile
  query
  query-one
  reducible-query
  with-call-count]

 [toucan2.insert
  insert!
  insert-returning-instances!
  insert-returning-pks!
  reducible-insert
  reducible-insert-returning-instances
  reducible-insert-returning-pks]

 [toucan2.instance
  changes
  current
  instance
  instance-of?
  instance?
  model
  original]

 [toucan2.model
  default-connectable
  primary-key-values
  primary-keys
  primary-keys-vec
  table-name
  with-model]

 [toucan2.save
  save!]

 [toucan2.select
  count
  exists?
  select
  select-fn->fn
  select-fn->pk
  select-fn-reducible
  select-fn-set
  select-fn-vec
  select-one
  select-one-fn
  select-one-pk
  select-pk->fn
  select-pks-fn
  select-pks-set
  select-pks-vec
  select-reducible]

 [toucan2.tools.after-insert
  define-after-insert]

 [toucan2.tools.after-update
  define-after-update]

 [toucan2.tools.before-insert
  define-before-insert]

 [toucan2.tools.before-update
  define-before-update]

 #_[toucan2.tools.disallow]

 [toucan2.tools.helpers
  define-after-select-each
  define-after-select-reducible
  define-before-delete
  define-before-select
  define-default-fields
  deftransforms]

 [toucan2.tools.hydrate
  batched-hydrate
  fk-keys-for-automagic-hydration
  hydrate
  simple-hydrate
  table-for-automagic-hydration]

 [toucan2.tools.identity-query
  identity-query]

 #_[toucan2.tools.with-temp]

 [toucan2.tools.transformed
  transforms]

 [toucan2.update
  reducible-update
  reducible-update-returning-pks
  update!
  update-returning-pks!]

 [toucan2.util
  *debug*
  dispatch-value])
