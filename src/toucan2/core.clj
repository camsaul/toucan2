(ns ^:no-doc toucan2.core
  "Convenience namespace exposing the most common parts of the library's public API for day-to-day usage (i.e., not
  implementing anything advanced)"
  (:refer-clojure :exclude [compile count instance?])
  (:require
   [potemkin :as p]
   [toucan2.connection]
   [toucan2.delete]
   [toucan2.execute]
   [toucan2.honeysql2]
   [toucan2.insert]
   [toucan2.instance]
   [toucan2.jdbc]
   [toucan2.model]
   [toucan2.protocols]
   [toucan2.save]
   [toucan2.select]
   [toucan2.tools.after-insert]
   [toucan2.tools.after-select]
   [toucan2.tools.after-update]
   [toucan2.tools.before-delete]
   [toucan2.tools.before-insert]
   [toucan2.tools.before-select]
   [toucan2.tools.before-update]
   [toucan2.tools.compile]
   [toucan2.tools.debug]
   [toucan2.tools.default-fields]
   [toucan2.tools.hydrate]
   [toucan2.tools.named-query]
   [toucan2.tools.transformed]
   [toucan2.update]
   [toucan2.util]))

;;; this is so no one gets confused and thinks these namespaces are unused.
(comment
  toucan2.connection/keep-me
  toucan2.delete/keep-me
  toucan2.execute/keep-me
  toucan2.honeysql2/keep-me
  toucan2.insert/keep-me
  toucan2.instance/keep-me
  toucan2.jdbc/keep-me
  toucan2.model/keep-me
  toucan2.protocols/keep-me
  toucan2.save/keep-me
  toucan2.select/keep-me
  toucan2.tools.after-insert/keep-me
  toucan2.tools.after-select/keep-me
  toucan2.tools.after-update/keep-me
  toucan2.tools.before-delete/keep-me
  toucan2.tools.before-insert/keep-me
  toucan2.tools.before-select/keep-me
  toucan2.tools.before-update/keep-me
  toucan2.tools.compile/keep-me
  toucan2.tools.debug/keep-me
  toucan2.tools.default-fields/keep-me
  toucan2.tools.hydrate/keep-me
  toucan2.tools.named-query/keep-me
  toucan2.tools.transformed/keep-me
  toucan2.update/keep-me
  toucan2.util/keep-me)

(p/import-vars
 [toucan2.connection
  do-with-connection
  with-connection
  with-transaction]

 [toucan2.delete
  delete!]

 [toucan2.execute
  query
  query-one
  reducible-query
  with-call-count]

 [toucan2.insert
  insert!
  insert-returning-instances!
  insert-returning-pks!]

 [toucan2.instance
  instance
  instance-of?
  instance?]

 [toucan2.model
  default-connectable
  primary-key-values-map
  primary-keys
  resolve-model
  select-pks-fn
  table-name]

 [toucan2.protocols
  changes
  current
  model
  original]

 [toucan2.save
  save!]

 [toucan2.select
  count
  exists?
  reducible-select
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
  select-pks-set
  select-pks-vec]

 [toucan2.tools.after-insert
  define-after-insert]

 [toucan2.tools.after-select
  define-after-select]

 [toucan2.tools.after-update
  define-after-update]

 [toucan2.tools.before-delete
  define-before-delete]

 [toucan2.tools.before-insert
  define-before-insert]

 [toucan2.tools.before-select
  define-before-select]

 [toucan2.tools.before-update
  define-before-update]

 [toucan2.tools.compile
  build
  compile]

 [toucan2.tools.debug
  debug]

 [toucan2.tools.default-fields
  define-default-fields]

 [toucan2.tools.hydrate
  batched-hydrate
  hydrate
  model-for-automagic-hydration
  simple-hydrate]

 [toucan2.tools.named-query
  define-named-query]

 [toucan2.tools.transformed
  deftransforms
  transforms]

 [toucan2.update
  reducible-update
  reducible-update-returning-pks
  update!
  update-returning-pks!])
