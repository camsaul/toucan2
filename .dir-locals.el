((nil . ((indent-tabs-mode . nil)       ; always use spaces for tabs
         (require-final-newline . t)))  ; add final newline on save
 (clojure-mode . ((cider-preferred-build-tool . clojure-cli)
                  (cider-clojure-cli-aliases . "dev")
                  (cljr-favor-prefix-notation . nil)
                  (fill-column . 120)
                  (clojure-docstring-fill-column . 120)
                  (eval . (put 'p/defprotocol+ 'clojure-doc-string-elt 2))
                  (eval . (put 'p/def-map-type 'clojure-doc-string-elt 2))
                  (eval . (put-clojure-indent 'p/defprotocol+ '(1 (:defn))))
                  (eval . (put-clojure-indent 'p/def-map-type '(2 nil nil (:defn))))
                  (eval . (put-clojure-indent 'p/deftype+ '(2 nil nil (:defn))))
                  (eval . (put-clojure-indent 'with-meta '(:form)))
                  (eval . (put-clojure-indent 'with-bindings* '(:form))))))
