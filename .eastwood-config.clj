(disable-warning
 {:linter :unused-ret-vals-in-try
  :if-inside-macroexpansion-of #{'clojure.test/is}
  :within-depth 10
  :reason "A thrown? form can generate false positives for this linter."})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.compile/compile*
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-one
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-fn-set
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-fn-vec
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-one-fn
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-fn->pk
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-fn->fn
  :arglists-for-linting '([f1 f2 connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-pk->fn
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-one-pk
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-pks-set
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/select-pks-vec
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/exists?
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.select/count
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.mutative/update!
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'bluejdbc.mutative/insert!
  :arglists-for-linting '([connectable-tableable & args])})
