((nil . ((indent-tabs-mode . nil)       ; always use spaces for tabs
         (require-final-newline . t)))  ; add final newline on save
 (clojure-mode . ((eval . (progn
                            ;; Specify which arg is the docstring for certain macros
                            ;; (Add more as needed)
                            (put 's/defn 'clojure-doc-string-elt 2)
                            (put 'p.types/defprotocol+ 'clojure-doc-string-elt 2)

                            (define-clojure-indent
                              (p.types/defprotocol+ '(1 (:defn)))
                              (p.types/defrecord+ '(2 nil nil (:defn)))
                              (p.types/deftype+ '(2 nil nil (:defn)))
                              (insert! 1)
                              (transaction 2))))
                  ;; if you're using clj-refactor (highly recommended!), prefer prefix notation when cleaning the ns
                  ;; form
                  (cljr-favor-prefix-notation . nil)
                  ;; prefer keeping source width about ~118, GitHub seems to cut off stuff at either 119 or 120 and
                  ;; it's nicer to look at code in GH when you don't have to scroll back and forth
                  (fill-column . 118)
                  (clojure-docstring-fill-column . 118))))
