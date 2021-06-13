(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.compile/compile*
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.core/select
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-reducible
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-one
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-fn-set
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-fn-vec
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-one-fn
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-fn->pk
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-fn->fn
  :arglists-for-linting '([f1 f2 connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-pk->fn
  :arglists-for-linting '([f connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-one-pk
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-pks-set
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/select-pks-vec
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/exists?
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.select/count
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.mutative/update!
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.mutative/insert!
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.mutative/insert-returning-keys!
  :arglists-for-linting '([connectable-tableable & args])})

(disable-warning
 {:linter :wrong-arity
  :function-symbol 'toucan2.mutative/delete!
  :arglists-for-linting '([connectable-tableable & args])})
