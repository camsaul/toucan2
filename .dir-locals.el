((nil . ((indent-tabs-mode . nil)       ; always use spaces for tabs
         (require-final-newline . t)))  ; add final newline on save
 (clojure-mode . ((cider-preferred-build-tool . clojure-cli)
                  (cider-clojure-cli-aliases . "dev")
                  (cljr-favor-prefix-notation . nil)
                  (fill-column . 120)
                  (clojure-docstring-fill-column . 120)
                  (eval . (put 'p/defprotocol+ 'clojure-doc-string-elt 2))
                  (eval . (put 'm/defmulti 'clojure-doc-string-elt 2))
                  (eval . (put-clojure-indent 'p/defprotocol+ '(1 (:defn)))))))
