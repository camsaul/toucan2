# Differences from Toucan 1

## Models are now just plain keywords

## `defmodel` is no longer needed

In Toucan 2, a *model* is usually just a plain keyword, or even a plain string table name, or anything custom that
implements `table-name`. As such, there is no need to define model classes with `defmodel`. Usually you decide that
some namespaced keyword corresponds to some table in your database and go from there, implementing methods to
customize its behavior as need. Usually you'll want to implement `table-name`, unless the `name` portion of the
keyword matches the table name exactly:

```clj
(m/defmethod t2/table-name :models/user
  [_model]
  "core_user")
```

The optional `toucan2-toucan1` compatibility layer provides a `defmodel` macro that mostly matches the syntax of the
old `defmodel` and automatically creates an implementation of `table-name` for you, and derives that model from
`:toucan1/model` (to implement other special backwards-compatibility behavior).

## `IModel` protocol removed

## Models are no longer invokeable

## `models/add-type!` and `models/types`

`toucan.models/add-type!` has no direct equivalent; just define a map of

```
column-name -> direction -> transform-fn
```

somewhere, like this:

```
(def json-xform
  {:in  json-in
   :out json-out-with-keywordization})
```


`toucan.models/types` has been replaced with `toucan2.core/transforms`, which provides similar functionality but is
much more consistent in applying it. Example:

```clj
;; define the transform
(m/defmethod t2/transforms ::my-model
  [_model]
  {:some-field json-xform})
```

Note that nothing is stopping you from defining the transforms inline in the body of `transforms` itself; you can also
define common transforms, and derive models from them.

`transforms` automatically merges all transforms maps for all matching methods; so you can define lots of common
transforms this way and build models that use several of them. See documentation for more details. (TODO)

## Hydrate is now a multimethod

## 'Simple' methods

## `toucan.models/primary-key` is replaced with `primary-keys`

## `do-pre-update` etc removed

## `with-temp` syntax; `with-temp*` removed

## `define-before-insert`, etc.

## `define-property!` -- derive model

## `hydration-keys` => `t2/table-for-automagic-hydration`

## `toucan.db/*db-connection*` is now current `*connectable*` (TODO: not sure if this will be renamed)

## `toucan.db/connection` removed

`toucan.db/connection` is defines something you can use to get a `java.sql.Connection`, for example a database details
map or a `javax.sql.DataSource`; it was meant to be used with something like `clojure.java.jdbc/with-db-connection`,
which uses `with-open` under the hood, or `clojure.java.jdbc/get-connection` in combination with `with-open`. Toucan 2
connectables handle their own lifecycle in their implementations of `do-with-connection` so you should not be using
`with-open` yourself.

## No need to flush hydration key caches
