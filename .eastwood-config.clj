(disable-warning
 {:linter :unused-ret-vals-in-try
  :if-inside-macroexpansion-of #{'clojure.test/is}
  :within-depth 10
  :reason "A thrown? form can generate false positives for this linter."})
