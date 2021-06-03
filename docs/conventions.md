# Library Conventions

### Multimethod Naming

All multimethods end in `*`. Often there is a convenient version of a function intended for external use with flexible
arglists, such as `instance`, whose arities are:

```clj
(instance tableabl)
(instance tableable m)
(instance connectable tableable m)
(instance connectable tableable k v & more)
```

The convenient function parses the args and eventually calls the underlying `method*`, e.g. `instance*`:

```clj
(instance* connectable tableable original-map current-map key-xform metta)
```

When you're extending Blue JDBC, implement a `method*`, when you're using Blue JDBC, call a `function`.

### Parameter Order

`connectable`, `tableable`, `queryable`, `options`, and the like are common parameters that are used all over the
codebase. To keep things predictable, the following parameter order conventions are used everywhere:

* If `connectable` is an arg, it is almost always the (sometimes optional) first arg. Exceptions are functions like
  `select-pks-set` that place special meaning on the first arg.
* If `tableable` is an arg, it follows directly after `connectable`, i.e. is the second arg.
* Anything that takes a `tableable` should also take a `connectable`.
* If `options` is an arg, it is always the last argument.
